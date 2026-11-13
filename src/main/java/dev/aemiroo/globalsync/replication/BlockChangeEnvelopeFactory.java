package dev.aemiroo.globalsync.replication;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Converts a player block edit into the versioned replication event envelope. */
public final class BlockChangeEnvelopeFactory {
    public static final String BLOCK_PLACE = "BLOCK_PLACE";
    public static final String BLOCK_BREAK = "BLOCK_BREAK";

    private BlockChangeEnvelopeFactory() { }

    public static ReplicationEnvelope create(String eventType, String worldId,
            String originServerId, long sequence, long worldSequence, long logicalClock, UUID playerId,
            int x, int y, int z, String previousBlockData, String newBlockData) {
        if (!BLOCK_PLACE.equals(eventType) && !BLOCK_BREAK.equals(eventType))
            throw new IllegalArgumentException("Unsupported player block event type: " + eventType);
        return new ReplicationEnvelope(
                ReplicationEnvelope.CURRENT_PROTOCOL_VERSION, UUID.randomUUID(), worldId,
                originServerId, sequence, worldSequence, logicalClock, Instant.now(), eventType,
                Map.of("player_uuid", playerId.toString(),
                        "x", Integer.toString(x), "y", Integer.toString(y),
                        "z", Integer.toString(z),
                        "previous_block_data", previousBlockData,
                        "new_block_data", newBlockData));
    }

    /** Compatibility overload; production capture supplies a Lamport clock explicitly. */
    public static ReplicationEnvelope create(String eventType, String worldId,
            String originServerId, long sequence, UUID playerId,
            int x, int y, int z, String previousBlockData, String newBlockData) {
        return create(eventType, worldId, originServerId, sequence, sequence, sequence, playerId,
                x, y, z, previousBlockData, newBlockData);
    }
}
