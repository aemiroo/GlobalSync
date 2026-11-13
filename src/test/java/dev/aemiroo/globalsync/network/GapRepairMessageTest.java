package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.ReplicationEnvelope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class GapRepairMessageTest {
    @Test
    void roundTripsAnExactOriginSequenceRequest() throws Exception {
        UUID id = UUID.randomUUID();
        var decoded = GapRepairMessage.decode(GapRepairMessage.encodeRequest(id, "node-eu", 42));
        assertEquals(GapRepairMessage.Type.REQUEST, decoded.type());
        assertEquals(id, decoded.requestId());
        assertEquals("node-eu", decoded.originServerId());
        assertEquals(42, decoded.sequence());
    }

    @Test
    void roundTripsAnEventResponseWithoutChangingItsEnvelope() throws Exception {
        UUID requestId = UUID.randomUUID();
        ReplicationEnvelope event = event();
        var decoded = GapRepairMessage.decode(GapRepairMessage.encodeEvent(requestId, event));
        assertEquals(GapRepairMessage.Type.EVENT, decoded.type());
        assertEquals(requestId, decoded.requestId());
        assertEquals(event, ReplicationMessageCodec.decodeEvent(
                decoded.eventPayload().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                "node-eu", java.util.Set.of("world")));
    }

    @Test
    void roundTripsAnUnavailableResponse() throws Exception {
        UUID id = UUID.randomUUID();
        var decoded = GapRepairMessage.decode(GapRepairMessage.encodeUnavailable(id, "node-eu", 9));
        assertEquals(GapRepairMessage.Type.UNAVAILABLE, decoded.type());
        assertEquals(id, decoded.requestId());
        assertEquals("node-eu", decoded.originServerId());
        assertEquals(9, decoded.sequence());
        assertEquals(GapRepairMessage.UnavailableReason.NOT_RETAINED, decoded.reason());
    }

    @Test
    void rejectsUnknownFieldsWrongVersionInvalidIdsAndNonPositiveSequences() {
        UUID id = UUID.randomUUID();
        assertThrows(IllegalArgumentException.class, () -> GapRepairMessage.encodeRequest(id, "bad id", 1));
        assertThrows(IllegalArgumentException.class, () -> GapRepairMessage.encodeRequest(id, "node-eu", 0));
        assertThrows(java.io.IOException.class, () -> GapRepairMessage.decode(
                ("{\"type\":\"GAP_REQUEST\",\"protocolVersion\":1,\"requestId\":\""
                        + id + "\",\"originServerId\":\"node-eu\",\"sequence\":1}")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertThrows(java.io.IOException.class, () -> GapRepairMessage.decode(
                ("{\"type\":\"GAP_REQUEST\",\"protocolVersion\":2,\"requestId\":\""
                        + id + "\",\"originServerId\":\"node-eu\",\"sequence\":1,\"extra\":true}")
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    @Test
    void identifiesOnlyGapRepairFrames() {
        assertTrue(GapRepairMessage.isGapMessage(GapRepairMessage.encodeRequest(UUID.randomUUID(), "node-eu", 1)));
        assertFalse(GapRepairMessage.isGapMessage(ReplicationMessageCodec.encodeEvent(event())));
        assertFalse(GapRepairMessage.isGapMessage(null));
    }

    private static ReplicationEnvelope event() {
        return new ReplicationEnvelope(2, UUID.fromString("9eec1f2d-c8d8-4c85-92d9-4a6e3baf01b1"),
                "world", "node-eu", 4, Instant.parse("2026-11-13T00:00:00Z"), "BLOCK_PLACE",
                Map.of("player_uuid", "d6a9b5df-1b1d-4b17-9f45-9f9b9f1b7d11",
                        "x", "12", "y", "64", "z", "-4",
                        "previous_block_data", "minecraft:air", "new_block_data", "minecraft:stone"));
    }
}
