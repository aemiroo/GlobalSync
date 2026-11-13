package dev.aemiroo.globalsync.replication;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates independent, monotonically increasing event sequences per world.
 * Persisted values must be restored from the durable journal before capture starts.
 */
public final class WorldSequenceClock {
    private final Map<String, AtomicLong> sequences = new ConcurrentHashMap<>();

    public WorldSequenceClock() { }

    public WorldSequenceClock(Map<String, Long> initialSequences) {
        java.util.Objects.requireNonNull(initialSequences, "initialSequences").forEach((world, sequence) -> {
            validateWorld(world);
            if (sequence == null || sequence < 0) {
                throw new IllegalArgumentException("World sequence must not be negative");
            }
            sequences.put(world, new AtomicLong(sequence));
        });
    }

    public long next(String worldId) {
        validateWorld(worldId);
        return sequences.computeIfAbsent(worldId, ignored -> new AtomicLong()).incrementAndGet();
    }

    public long current(String worldId) {
        validateWorld(worldId);
        AtomicLong sequence = sequences.get(worldId);
        return sequence == null ? 0L : sequence.get();
    }

    private static void validateWorld(String worldId) {
        if (worldId == null || worldId.isBlank()) {
            throw new IllegalArgumentException("worldId must not be blank");
        }
    }
}
