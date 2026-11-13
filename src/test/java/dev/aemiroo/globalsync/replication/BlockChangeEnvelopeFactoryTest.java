package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BlockChangeEnvelopeFactoryTest {
    @Test
    void createsAPlacementEventWithLocationAndBeforeAfterBlockData() {
        UUID playerId = UUID.randomUUID();

        ReplicationEnvelope envelope = BlockChangeEnvelopeFactory.create(
                BlockChangeEnvelopeFactory.BLOCK_PLACE,
                "world",
                "region-asia",
                17,
                playerId,
                12,
                64,
                -8,
                "minecraft:air",
                "minecraft:oak_planks"
        );

        assertEquals(ReplicationEnvelope.CURRENT_PROTOCOL_VERSION, envelope.protocolVersion());
        assertEquals("world", envelope.worldId());
        assertEquals("region-asia", envelope.originServerId());
        assertEquals(17, envelope.sequence());
        assertEquals("BLOCK_PLACE", envelope.eventType());
        assertEquals(playerId.toString(), envelope.attributes().get("player_uuid"));
        assertEquals("12", envelope.attributes().get("x"));
        assertEquals("64", envelope.attributes().get("y"));
        assertEquals("-8", envelope.attributes().get("z"));
        assertEquals("minecraft:air", envelope.attributes().get("previous_block_data"));
        assertEquals("minecraft:oak_planks", envelope.attributes().get("new_block_data"));
        assertNotNull(envelope.eventId());
        assertNotNull(envelope.createdAt());
    }

    @Test
    void createsBreakEventsWithTheExpectedAirResult() {
        ReplicationEnvelope envelope = BlockChangeEnvelopeFactory.create(
                BlockChangeEnvelopeFactory.BLOCK_BREAK,
                "world",
                "region-eu",
                2,
                UUID.randomUUID(),
                0,
                70,
                0,
                "minecraft:stone",
                "minecraft:air"
        );

        assertEquals("BLOCK_BREAK", envelope.eventType());
        assertEquals("minecraft:stone", envelope.attributes().get("previous_block_data"));
        assertEquals("minecraft:air", envelope.attributes().get("new_block_data"));
    }

    @Test
    void rejectsEventTypesOutsideThePlayerBlockCaptureScope() {
        assertThrows(IllegalArgumentException.class, () -> BlockChangeEnvelopeFactory.create(
                "EXPLOSION",
                "world",
                "region-asia",
                1,
                UUID.randomUUID(),
                0,
                64,
                0,
                "minecraft:air",
                "minecraft:air"
        ));
    }
}
