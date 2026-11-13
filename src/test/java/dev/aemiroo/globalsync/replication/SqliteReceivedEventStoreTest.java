package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
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

            assertEquals(List.of(alpha.eventId()), first.pendingWinners().stream()
                    .map(ReplicationEnvelope::eventId).toList());
            assertEquals(List.of(alpha.eventId()), second.pendingWinners().stream()
                    .map(ReplicationEnvelope::eventId).toList());
            assertTrue(first.isCurrentWinner(alpha.eventId()));
            assertFalse(first.isCurrentWinner(zeta.eventId()));
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
            assertTrue(store.applyCurrentWinner(causallyLater.eventId(), mutations::incrementAndGet));
            assertFalse(store.applyCurrentWinner(causallyLater.eventId(), mutations::incrementAndGet));
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
