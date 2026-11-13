package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.ReplicationEnvelope;
import dev.aemiroo.globalsync.replication.SqliteReceivedEventStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationMessageHandlerTest {
    @TempDir Path directory;

    @Test
    void acknowledgesOnlyAfterDurableAppendAndTreatsRetriesIdempotently() throws Exception {
        Path database = directory.resolve("events.db");
        ReplicationEnvelope event = event();
        try (SqliteReceivedEventStore journal = new SqliteReceivedEventStore(database)) {
            ReplicationMessageHandler handler = new ReplicationMessageHandler(journal, Set.of("world"));
            byte[] payload = ReplicationMessageCodec.encodeEvent(event);

            byte[] firstAck = handler.handle(payload, "region-eu");
            ReplicationMessageCodec.validateAck(firstAck, event.eventId());
            assertEquals(1, journal.size());

            byte[] retryAck = handler.handle(payload, "region-eu");
            ReplicationMessageCodec.validateAck(retryAck, event.eventId());
            assertEquals(1, journal.size());
        }

        // The event remains deduplicated after the receiver process/database is reopened.
        try (SqliteReceivedEventStore recovered = new SqliteReceivedEventStore(database)) {
            ReplicationMessageHandler handler = new ReplicationMessageHandler(recovered, Set.of("world"));
            ReplicationMessageCodec.validateAck(handler.handle(
                    ReplicationMessageCodec.encodeEvent(event), "region-eu"), event.eventId());
            assertEquals(1, recovered.size());
        }
    }

    @Test
    void rejectsOriginSpoofingAndUnsharedWorlds() throws Exception {
        try (SqliteReceivedEventStore journal = new SqliteReceivedEventStore(directory.resolve("received.db"))) {
            ReplicationMessageHandler handler = new ReplicationMessageHandler(journal, Set.of("world"));
            assertThrows(java.io.IOException.class, () -> handler.handle(
                    ReplicationMessageCodec.encodeEvent(event()), "different-peer"));
            ReplicationEnvelope otherWorld = new ReplicationEnvelope(ReplicationEnvelope.CURRENT_PROTOCOL_VERSION, UUID.randomUUID(), "other",
                    "region-eu", 2, Instant.now(), "BLOCK_PLACE", event().attributes());
            assertThrows(java.io.IOException.class, () -> handler.handle(
                    ReplicationMessageCodec.encodeEvent(otherWorld), "region-eu"));
            assertEquals(0, journal.size());
        }
    }

    @Test
    void rejectsUnknownFieldsAndUnsupportedEventTypes() throws Exception {
        try (SqliteReceivedEventStore journal = new SqliteReceivedEventStore(directory.resolve("received.db"))) {
            ReplicationMessageHandler handler = new ReplicationMessageHandler(journal, Set.of("world"));
            String valid = new String(ReplicationMessageCodec.encodeEvent(event()), StandardCharsets.UTF_8);
            String unknown = valid.substring(0, valid.length() - 1) + ",\"remoteCommand\":\"op *\"}";
            assertThrows(java.io.IOException.class, () -> handler.handle(
                    unknown.getBytes(StandardCharsets.UTF_8), "region-eu"));
            String duplicateKey = valid.replaceFirst("\"type\":\"EVENT\"", "\"type\":\"EVENT\",\"type\":\"EVENT\"");
            assertThrows(java.io.IOException.class, () -> handler.handle(
                    duplicateKey.getBytes(StandardCharsets.UTF_8), "region-eu"));
            ReplicationEnvelope unsupported = new ReplicationEnvelope(ReplicationEnvelope.CURRENT_PROTOCOL_VERSION, UUID.randomUUID(), "world",
                    "region-eu", 3, Instant.now(), "ENTITY_TELEPORT", Map.of());
            assertThrows(java.io.IOException.class, () -> handler.handle(
                    ReplicationMessageCodec.encodeEvent(unsupported), "region-eu"));
            assertEquals(0, journal.size());
        }
    }

    @Test
    void rejectsConflictingReuseOfAnEventIdOrOriginSequence() throws Exception {
        try (SqliteReceivedEventStore journal = new SqliteReceivedEventStore(directory.resolve("received.db"))) {
            ReplicationMessageHandler handler = new ReplicationMessageHandler(journal, Set.of("world"));
            ReplicationEnvelope original = event();
            handler.handle(ReplicationMessageCodec.encodeEvent(original), "region-eu");
            ReplicationEnvelope conflict = new ReplicationEnvelope(ReplicationEnvelope.CURRENT_PROTOCOL_VERSION, original.eventId(), "world",
                    "region-eu", original.sequence(), Instant.now(), "BLOCK_PLACE",
                    Map.of("player_uuid", original.attributes().get("player_uuid"),
                            "x", "99", "y", "64", "z", "8",
                            "previous_block_data", "minecraft:air",
                            "new_block_data", "minecraft:stone"));
            assertThrows(java.io.IOException.class, () -> handler.handle(
                    ReplicationMessageCodec.encodeEvent(conflict), "region-eu"));
            assertEquals(1, journal.size());
        }
    }

    @Test
    void rejectsPeerFloodBeforeWritingToTheInbox() throws Exception {
        try (SqliteReceivedEventStore journal = new SqliteReceivedEventStore(directory.resolve("limited.db"))) {
            PeerMessageRateLimiter limiter = new PeerMessageRateLimiter(
                    4, 1.0, 1.0, 10_000.0, 20_000.0);
            ReplicationMessageHandler handler = new ReplicationMessageHandler(
                    journal, Set.of("world"), null, ignored -> { }, limiter);
            byte[] payload = ReplicationMessageCodec.encodeEvent(event());

            ReplicationMessageCodec.validateAck(handler.handle(payload, "region-eu"), event().eventId());
            assertThrows(java.io.IOException.class, () -> handler.handle(payload, "region-eu"));
            assertEquals(1, journal.size(), "rate-limited frames must not reach durable inbox storage");
        }
    }

    @Test
    void gapResponseMustMatchRequestedSequenceBeforeDurableAcceptance() throws Exception {
        try (SqliteReceivedEventStore journal = new SqliteReceivedEventStore(directory.resolve("gap-response.db"))) {
            ReplicationMessageHandler handler = new ReplicationMessageHandler(journal, Set.of("world"));
            byte[] payload = ReplicationMessageCodec.encodeEvent(event());
            assertThrows(java.io.IOException.class, () -> handler.handleGapResponse(payload, "region-eu", 2));
            assertEquals(0, journal.size(), "a mismatched targeted response must not enter the durable inbox");
            assertEquals(event().eventId(), handler.handleGapResponse(payload, "region-eu", 1));
            assertEquals(1, journal.size());
        }
    }

    private static ReplicationEnvelope event() {
        return new ReplicationEnvelope(ReplicationEnvelope.CURRENT_PROTOCOL_VERSION, UUID.fromString("9eec1f2d-c8d8-4c85-92d9-4a6e3baf01b1"),
                "world", "region-eu", 1, Instant.parse("2026-11-13T00:00:00Z"), "BLOCK_PLACE",
                Map.of("player_uuid", "d6a9b5df-1b1d-4b17-9f45-9f9b9f1b7d11",
                        "x", "12", "y", "64", "z", "-4",
                        "previous_block_data", "minecraft:air",
                        "new_block_data", "minecraft:stone"));
    }
}