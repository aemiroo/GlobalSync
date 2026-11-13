package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SqliteReceivedEventStoreTest {
    @TempDir Path directory;

    @Test
    void concurrentEventsChooseTheSameWinnerRegardlessOfArrivalOrder() throws Exception {
        ReplicationEnvelope alpha = event("region-alpha", 1, 8, "minecraft:dirt");
        ReplicationEnvelope zeta = event("region-zeta", 1, 8, "minecraft:stone");

        try (SqliteReceivedEventStore first = new SqliteReceivedEventStore(directory.resolve("first.db"));
             SqliteReceivedEventStore second = new SqliteReceivedEventStore(directory.resolve("second.db"))) {
            first.accept(zeta);
            first.accept(alpha);
            second.accept(alpha);
            second.accept(zeta);

            assertEquals(List.of(zeta.eventId()), first.pendingWinners().stream()
                    .map(ReplicationEnvelope::eventId).toList());
            assertEquals(List.of(zeta.eventId()), second.pendingWinners().stream()
                    .map(ReplicationEnvelope::eventId).toList());
            assertTrue(first.isCurrentWinner(zeta.eventId()));
            assertFalse(first.isCurrentWinner(alpha.eventId()));
        }
    }

    @Test
    void laterLamportEventSupersedesConcurrentTieAndApplicationIsIdempotent() throws Exception {
        try (SqliteReceivedEventStore store = new SqliteReceivedEventStore(directory.resolve("received.db"))) {
            ReplicationEnvelope concurrentWinner = event("region-zeta", 1, 12, "minecraft:stone");
            ReplicationEnvelope causallyLater = event("region-alpha", 1, 13, "minecraft:oak_planks");

            assertTrue(store.accept(concurrentWinner).applyRequired());
            assertTrue(store.accept(causallyLater).applyRequired());
            assertEquals(List.of(causallyLater.eventId()),
                    store.pendingWinners().stream().map(ReplicationEnvelope::eventId).toList());

            AtomicInteger mutations = new AtomicInteger();
            assertFalse(store.applyCurrentWinner(causallyLater.eventId(), () -> false));
            assertEquals(1, store.pendingWinners().size());
            assertTrue(store.applyCurrentWinner(causallyLater.eventId(), () -> { mutations.incrementAndGet(); return true; }));
            assertFalse(store.applyCurrentWinner(causallyLater.eventId(), () -> { mutations.incrementAndGet(); return true; }));
            assertEquals(1, mutations.get());
            assertTrue(store.pendingWinners().isEmpty());
        }
    }

    @Test
    void localEventsAreRecordedAsAlreadyAppliedAndClockSurvivesRestart() throws Exception {
        Path database = directory.resolve("local.db");
        ReplicationEnvelope local = event("region-local", 1, 27, "minecraft:gold_block");
        try (SqliteReceivedEventStore store = new SqliteReceivedEventStore(database)) {
            assertFalse(store.acceptLocal(local).applyRequired());
            assertEquals(27, store.lastLogicalClock());
            assertTrue(store.pendingWinners().isEmpty());
        }
        try (SqliteReceivedEventStore recovered = new SqliteReceivedEventStore(database)) {
            assertEquals(27, recovered.lastLogicalClock());
            assertTrue(recovered.isCurrentWinner(local.eventId()));
        }
    }




    @Test
    void localJournalIndexCheckpointAdvancesAndSurvivesRestart() throws Exception {
        Path database = directory.resolve("checkpoint.db");
        ReplicationEnvelope local = event("region-local", 1, 1, "minecraft:stone");
        ReplicationEnvelope metadata = new ReplicationEnvelope(
                ReplicationEnvelope.CURRENT_PROTOCOL_VERSION, UUID.randomUUID(), "world",
                "region-local", 2, 2, Instant.parse("2026-11-13T00:00:00Z"),
                "IGNORED_LOCAL_METADATA", Map.of("value", "safe"));
        try (SqliteReceivedEventStore store = new SqliteReceivedEventStore(database)) {
            assertEquals(0, store.lastIndexedLocalJournalOffset());
            store.indexLocalJournalEntry(local, 1);
            store.indexLocalJournalEntry(metadata, 2);
            assertEquals(2, store.lastIndexedLocalJournalOffset());
            assertTrue(store.isCurrentWinner(local.eventId()));
        }
        try (SqliteReceivedEventStore recovered = new SqliteReceivedEventStore(database)) {
            assertEquals(2, recovered.lastIndexedLocalJournalOffset());
        }
    }

    @Test
    void migratesPreviouslyReceivedV1EventsAndRecognizesTheirV2Replay() throws Exception {
        Path database = directory.resolve("legacy.db");
        String eventId = "9eec1f2d-c8d8-4c85-92d9-4a6e3baf01b1";
        String playerId = "d6a9b5df-1b1d-4b17-9f45-9f9b9f1b7d11";
        String payload = "{\"type\":\"EVENT\",\"protocolVersion\":1,\"eventId\":\"" + eventId
                + "\",\"worldId\":\"world\",\"originServerId\":\"region-old\",\"sequence\":7"
                + ",\"createdAt\":\"2026-11-13T00:00:00Z\",\"eventType\":\"BLOCK_PLACE\""
                + ",\"attributes\":{\"new_block_data\":\"minecraft:stone\",\"player_uuid\":\""
                + playerId + "\",\"previous_block_data\":\"minecraft:air\",\"x\":\"12\""
                + ",\"y\":\"64\",\"z\":\"-4\"}}";
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE received_events (
                        receipt_offset INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id TEXT NOT NULL UNIQUE,
                        origin_server_id TEXT NOT NULL,
                        origin_sequence INTEGER NOT NULL CHECK (origin_sequence > 0),
                        world_id TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        UNIQUE (origin_server_id, origin_sequence)
                    )
                    """);
            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO received_events
                    (event_id, origin_server_id, origin_sequence, world_id, payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """)) {
                insert.setString(1, eventId);
                insert.setString(2, "region-old");
                insert.setLong(3, 7);
                insert.setString(4, "world");
                insert.setString(5, payload);
                insert.executeUpdate();
            }
        }

        try (SqliteReceivedEventStore migrated = new SqliteReceivedEventStore(database)) {
            ReplicationEnvelope event = migrated.pendingWinners().getFirst();
            assertEquals(7, event.logicalClock());
            assertEquals(UUID.fromString(eventId), event.eventId());
            assertEquals(7, migrated.lastLogicalClock());
            var replay = migrated.accept(event);
            assertTrue(replay.duplicate());
            assertTrue(replay.applyRequired());
        }
    }

    @Test
    void pendingRemoteWinnerSurvivesInboxRestart() throws Exception {
        Path database = directory.resolve("pending.db");
        ReplicationEnvelope remote = event("region-remote", 4, 41, "minecraft:glass");
        try (SqliteReceivedEventStore store = new SqliteReceivedEventStore(database)) {
            assertTrue(store.accept(remote).applyRequired());
            assertEquals(41, store.lastLogicalClock());
        }
        try (SqliteReceivedEventStore recovered = new SqliteReceivedEventStore(database)) {
            assertEquals(41, recovered.lastLogicalClock());
            assertEquals(List.of(remote.eventId()),
                    recovered.pendingWinners().stream().map(ReplicationEnvelope::eventId).toList());
        }
    }

    @Test
    void exactRetriesRemainIdempotentAndConflictingReuseIsRejected() throws Exception {
        try (SqliteReceivedEventStore store = new SqliteReceivedEventStore(directory.resolve("retry.db"))) {
            ReplicationEnvelope original = event("region-alpha", 1, 1, "minecraft:stone");
            assertFalse(store.accept(original).duplicate());
            assertTrue(store.accept(original).duplicate());
            ReplicationEnvelope conflict = new ReplicationEnvelope(
                    ReplicationEnvelope.CURRENT_PROTOCOL_VERSION, original.eventId(), "world",
                    "region-alpha", 1, 1, Instant.parse("2026-11-13T00:00:00Z"),
                    "BLOCK_PLACE", attributes("minecraft:dirt"));
            assertThrows(JournalConflictException.class, () -> store.accept(conflict));
            assertEquals(1, store.size());
        }
    }

    private static ReplicationEnvelope event(String origin, long sequence, long clock, String blockData) {
        return new ReplicationEnvelope(ReplicationEnvelope.CURRENT_PROTOCOL_VERSION, UUID.randomUUID(),
                "world", origin, sequence, clock, Instant.parse("2026-11-13T00:00:00Z"),
                "BLOCK_PLACE", attributes(blockData));
    }

    private static Map<String, String> attributes(String blockData) {
        return Map.of("player_uuid", "d6a9b5df-1b1d-4b17-9f45-9f9b9f1b7d11",
                "x", "12", "y", "64", "z", "-4",
                "previous_block_data", "minecraft:air", "new_block_data", blockData);
    }
}
