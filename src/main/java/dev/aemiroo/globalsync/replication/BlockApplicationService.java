package dev.aemiroo.globalsync.replication;

import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.block.data.BlockData;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Applies durable winning block events on Paper's main thread while keeping
 * SQLite reads/writes on a bounded background worker.
 */
public final class BlockApplicationService implements AutoCloseable {
    private static final int MAX_ATTEMPTS_PER_DRAIN = 32;

    private final JavaPlugin plugin;
    private final SqliteReceivedEventStore store;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean drainQueued = new AtomicBoolean();
    private final AtomicInteger pendingCursor = new AtomicInteger();
    private final ThreadPoolExecutor worker;
    private BukkitTask retryTask;

    public BlockApplicationService(JavaPlugin plugin, SqliteReceivedEventStore store) {
        this.plugin = Objects.requireNonNull(plugin, "plugin");
        this.store = Objects.requireNonNull(store, "store");
        worker = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(256), task -> {
                    Thread thread = new Thread(task, "GlobalSync-Block-Apply");
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        retryTask = Bukkit.getScheduler().runTaskTimer(plugin, this::scheduleDrain, 1L, 20L);
    }

    /** Safe to call from a peer worker; no Bukkit world API is used by that worker. */
    public void requestApply(ReplicationEnvelope event) {
        if (!running.get()) return;
        try {
            worker.execute(() -> applyOne(event));
        } catch (java.util.concurrent.RejectedExecutionException exception) {
            plugin.getLogger().warning("Block application queue is full; event remains durable and will be retried.");
        }
    }

    /**
     * Reconciles a local edit after its asynchronous journal commit. Paper applied
     * it before the winner index was durable, so a racing remote task may need to
     * be corrected if this local event is still the selected winner.
     */
    public void requestReconcile(ReplicationEnvelope event) {
        if (!running.get()) return;
        try {
            worker.execute(() -> {
                try {
                    store.reconcileCurrentWinner(event.eventId(), () -> applyOnPaperThread(event));
                } catch (SQLException | RuntimeException exception) {
                    plugin.getLogger().log(Level.SEVERE,
                            "Could not reconcile local block event " + event.eventId(), exception);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException exception) {
            plugin.getLogger().warning("Block reconciliation queue is full; local journal recovery will retry.");
        }
    }

    /** Called by the Paper scheduler; only enqueues background work. */
    private void scheduleDrain() {
        if (!running.get() || !drainQueued.compareAndSet(false, true)) return;
        try {
            worker.execute(() -> {
                try {
                    List<ReplicationEnvelope> pending = store.pendingWinners();
                    if (!pending.isEmpty()) {
                        int start = Math.floorMod(pendingCursor.get(), pending.size());
                        int attempts = Math.min(MAX_ATTEMPTS_PER_DRAIN, pending.size());
                        for (int i = 0; i < attempts && running.get(); i++) {
                            ReplicationEnvelope event = pending.get((start + i) % pending.size());
                            applyOne(event);
                        }
                        pendingCursor.set((start + attempts) % pending.size());
                    }
                } catch (SQLException exception) {
                    plugin.getLogger().log(Level.SEVERE, "Could not read pending block applications", exception);
                } finally {
                    drainQueued.set(false);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException exception) {
            drainQueued.set(false);
            plugin.getLogger().warning("Block application worker is busy; pending events will be retried.");
        }
    }

    /** Runs database winner checks off-thread and waits for the Paper-thread mutation. */
    private boolean applyOne(ReplicationEnvelope event) {
        if (!running.get()) return false;
        try {
            return store.applyCurrentWinner(event.eventId(), () -> applyOnPaperThread(event));
        } catch (SQLException | RuntimeException exception) {
            plugin.getLogger().log(Level.SEVERE,
                    "Could not apply winning block event " + event.eventId(), exception);
            return false;
        }
    }

    private boolean applyOnPaperThread(ReplicationEnvelope event) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        try {
            Bukkit.getScheduler().runTask(plugin, () -> {
                if (!running.get()) {
                    result.complete(false);
                    return;
                }
                try {
                    World world = Bukkit.getWorld(event.worldId());
                    if (world == null) {
                        result.complete(false);
                        return;
                    }
                    int x = Integer.parseInt(event.attributes().get("x"));
                    int y = Integer.parseInt(event.attributes().get("y"));
                    int z = Integer.parseInt(event.attributes().get("z"));
                    if (y < world.getMinHeight() || y >= world.getMaxHeight()
                            || !world.isChunkLoaded(x >> 4, z >> 4)) {
                        result.complete(false);
                        return;
                    }

                    BlockData blockData = Bukkit.createBlockData(resultingData);
                    Block block = world.getBlockAt(x, y, z);
                    String expectedPrevious = event.attributes().get("previous_block_data");
                    String resultingData = event.attributes().get("new_block_data");
                    String actual = block.getBlockData().getAsString();
                    if (actual.equals(resultingData)) {
                        result.complete(true);
                        return;
                    }
                    if (!actual.equals(expectedPrevious)) {
                        plugin.getLogger().warning("Block conflict at " + world.getName() + " "
                                + x + "," + y + "," + z + "; applying deterministic winner "
                                + event.eventId());
                    }
                    // Physics is disabled to avoid unjournaled neighbor changes.
                    block.setBlockData(blockData, false);
                    result.complete(true);
                } catch (RuntimeException exception) {
                    result.completeExceptionally(exception);
                }
            });
            return result.get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Paper block application", exception);
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException("Paper block application failed", cause);
        } catch (RuntimeException exception) {
            throw new IllegalStateException("Could not schedule Paper block application", exception);
        }
    }

    @Override
    public void close() {
        if (!running.getAndSet(false)) return;
        if (retryTask != null) {
            retryTask.cancel();
            retryTask = null;
        }
        worker.shutdownNow();
        try {
            worker.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
