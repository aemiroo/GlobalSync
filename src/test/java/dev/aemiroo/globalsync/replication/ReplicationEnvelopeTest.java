package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationEnvelopeTest {
    @Test
    void validatesAndCopiesEnvelopeAttributes() {
        var envelope = new ReplicationEnvelope(
                1, UUID.randomUUID(), "overworld", "asia", 1,
                Instant.now(), "BLOCK_CHANGE", Map.of("block", "minecraft:stone")
        );
        assertEquals("overworld", envelope.worldId());
        assertThrows(UnsupportedOperationException.class,
                () -> envelope.attributes().put("other", "value"));
    }

    @Test
    void rejectsUnknownProtocolVersions() {
        assertThrows(IllegalArgumentException.class, () -> new ReplicationEnvelope(
                99, UUID.randomUUID(), "overworld", "asia", 1,
                Instant.now(), "BLOCK_CHANGE", Map.of()
        ));
    }
}
