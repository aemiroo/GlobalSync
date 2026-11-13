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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Applies durable winning block events exclusively on Paper's main thread.
 * Unloaded worlds/chunks remain pending and are retried without force-loading chunks.
 */
public final class BlockApplicationService implements AutoCloseable {
    private static final int MAX_APPLICATIONS_PER_TICK = 32;

    private final JavaPlugin plugin;
    private final SqliteReceivedEventStore store;
    private final AtomicBoolean running = new AtomicBoolean();
    private BukkitTask retryTask;

    public BlockApplicationService(JavaPlugin plugin, SqliteReceivedEventStore store) {
        this.plugin = Objects.requireNonNull(plugin, "plugin");
        this.store = Objects.requireNonNull(store, "store");
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        retryTask = Bukkit.getScheduler().runTaskTimer(plugin, this::drainPending, 1L, 20L);
    }

    /** Safe to call from a peer worker; Bukkit world access is deferred to the main thread. */
    public void requestApply(ReplicationEnvelope event) {
        if (!running.get()) return;
        Bukkit.getScheduler().runTask(plugin, () -> applyOne(event));
    }

    private void drainPending() {
        if (!running.get()) return;
        try {
            List<ReplicationEnvelope> pending = store.pendingWinners();
            int applied = 0;
            for (ReplicationEnvelope event : pending) {
                if (applied >= MAX_APPLICATIONS_PER_TICK) break;
                if (applyOne(event)) applied++;
            }
        } catch (SQLException exception) {
            plugin.getLogger().log(Level.SEVERE, "Could not read pending block applications", exception);
        }
    }

    private boolean applyOne(ReplicationEnvelope event) {
        if (!running.get()) return false;
        World world = Bukkit.getWorld(event.worldId());
        if (world == null) return false;

        try {
            int x = Integer.parseInt(event.attributes().get("x"));
            int y = Integer.parseInt(event.attributes().get("y"));
            int z = Integer.parseInt(event.attributes().get("z"));
            if (y < world.getMinHeight() || y >= world.getMaxHeight()) {
                plugin.getLogger().warning("Deferring block event outside world height: " + event.eventId());
                return false;
            }
            if (!world.isChunkLoaded(x >> 4, z >> 4)) return false;

            String expectedPrevious = event.attributes().get("previous_block_data");
            String resultingData = event.attributes().get("new_block_data");
            BlockData blockData = Bukkit.createBlockData(resultingData);
            Block block = world.getBlockAt(x, y, z);

            return store.applyCurrentWinner(event.eventId(), () -> {
                String actual = block.getBlockData().getAsString();
                if (!actual.equals(expectedPrevious)) {
                    plugin.getLogger().warning("Block conflict at " + world.getName() + " "
                            + x + "," + y + "," + z + "; applying deterministic winner "
                            + event.eventId());
                }
                // Disable physics so one replicated edit does not create unjournaled
                // neighbor updates on this node.
                block.setBlockData(blockData, false);
            });
        } catch (SQLException | RuntimeException exception) {
            plugin.getLogger().log(Level.SEVERE,
                    "Could not apply winning block event " + event.eventId(), exception);
            return false;
        }
    }

    @Override
    public void close() {
        if (!running.getAndSet(false)) return;
        if (retryTask != null) {
            retryTask.cancel();
            retryTask = null;
        }
    }
}
