package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.ReplicationEnvelope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationMessageCodecWorldSequenceTest {
    @Test
    void roundTripsTheWorldSequenceSeparatelyFromTheOriginSequence() throws Exception {
        UUID player = UUID.randomUUID();
        ReplicationEnvelope event = new ReplicationEnvelope(3, UUID.randomUUID(), "overworld", "asia",
                41, 7, 52, Instant.parse("2026-11-13T00:00:00Z"), "BLOCK_PLACE",
                Map.of("player_uuid", player.toString(), "x", "1", "y", "64", "z", "2",
                        "previous_block_data", "minecraft:air", "new_block_data", "minecraft:stone"));

        ReplicationEnvelope decoded = ReplicationMessageCodec.decodeEvent(
                ReplicationMessageCodec.encodeEvent(event), "asia", Set.of("overworld"));

        assertEquals(41, decoded.sequence());
        assertEquals(7, decoded.worldSequence());
        assertEquals(event, decoded);
    }
}
