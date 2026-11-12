package dev.aemiroo.globalsync.replication;

import java.util.Objects;

/** Identifies a chunk within a world. */
public record ChunkKey(String worldId, int x, int z) {
    public ChunkKey {
        Objects.requireNonNull(worldId, "worldId");
        if (worldId.isBlank()) {
            throw new IllegalArgumentException("worldId must not be blank");
        }
    }
}
