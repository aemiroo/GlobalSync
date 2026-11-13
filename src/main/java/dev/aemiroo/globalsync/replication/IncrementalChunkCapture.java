package dev.aemiroo.globalsync.replication;

import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.ChunkSnapshot;
import org.bukkit.World;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Main-thread, bounded capture of already-loaded chunk snapshots.
 *
 * <p>This is deliberately a capture primitive, not a publishable world
 * snapshot. Chunks are sampled over multiple ticks and therefore do not share
 * a single atomic world-time boundary. Callers must not create a manifest or
 * advertise the result as a consistent snapshot until event reconciliation
 * and a consistency protocol are implemented.</p>
 */
public final class IncrementalChunkCapture {
    public static final int DEFAULT_CHUNKS_PER_TICK = 1;
    public static final int MAX_CHUNKS_PER_TICK = 8;
    public static final int MAX_CHUNKS_PER_CAPTURE = 65_536;

    public record CapturedChunk(int x, int z, ChunkSnapshot snapshot) {
        public CapturedChunk {
            Objects.requireNonNull(snapshot, "snapshot");
        }
    }

    public record Result(int capturedChunks, int skippedChunks) { }

    private final JavaPlugin plugin;

    public IncrementalChunkCapture(JavaPlugin plugin) {
        this.plugin = Objects.requireNonNull(plugin, "plugin");
    }

    /**
     * Captures only chunks that are loaded when this method is called.
     * No chunk is force-loaded or generated. The consumer runs on the Paper
     * main thread and must copy/enqueue the immutable snapshot quickly.
     */
    public CompletableFuture<Result> captureLoadedChunks(
            World world,
            int chunksPerTick,
            Consumer<CapturedChunk> consumer
    ) {
        Objects.requireNonNull(world, "world");
        Objects.requireNonNull(consumer, "consumer");
        if (!Bukkit.isPrimaryThread()) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Chunk capture must be started on the Paper main thread"));
        }
        if (chunksPerTick < 1 || chunksPerTick > MAX_CHUNKS_PER_TICK) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("chunksPerTick must be between 1 and " + MAX_CHUNKS_PER_TICK));
        }

        final List<Chunk> loaded = new ArrayList<>(List.of(world.getLoadedChunks()));
        if (loaded.size() > MAX_CHUNKS_PER_CAPTURE) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Loaded chunk count exceeds the snapshot capture safety limit"));
        }
        loaded.sort(Comparator.comparingInt(Chunk::getX).thenComparingInt(Chunk::getZ));

        CompletableFuture<Result> result = new CompletableFuture<>();
        AtomicBoolean finished = new AtomicBoolean();
        int[] index = {0};
        int[] captured = {0};
        BukkitTask task = Bukkit.getScheduler().runTaskTimer(plugin, () -> {
            if (finished.get()) return;
            try {
                int processed = 0;
                while (processed < chunksPerTick && index[0] < loaded.size()) {
                    Chunk chunk = loaded.get(index[0]++);
                    // A chunk may have unloaded after the initial inventory.
                    if (!chunk.isLoaded()) {
                        processed++;
                        continue;
                    }
                    ChunkSnapshot snapshot = chunk.getChunkSnapshot(
                            true, false, false);
                    consumer.accept(new CapturedChunk(chunk.getX(), chunk.getZ(), snapshot));
                    captured[0]++;
                    processed++;
                }
                if (index[0] >= loaded.size() && finished.compareAndSet(false, true)) {
                    result.complete(new Result(captured[0], loaded.size() - captured[0]));
                }
            } catch (Throwable failure) {
                if (finished.compareAndSet(false, true)) {
                    result.completeExceptionally(failure);
                }
            }
        }, 1L, 1L);

        result.whenComplete((ignored, failure) -> {
            if (!task.isCancelled()) task.cancel();
        });
        return result;
    }
}
