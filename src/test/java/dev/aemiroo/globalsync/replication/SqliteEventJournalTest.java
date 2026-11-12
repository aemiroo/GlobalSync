package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SqliteEventJournalTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void persistsEventsAndRestoresTheLastNodeSequenceAfterRestart() throws Exception {
        Path database = temporaryDirectory.resolve("journal/events.db");
        ReplicationEnvelope first = event(UUID.randomUUID(), "asia", 1, "BLOCK_CHANGE");
        ReplicationEnvelope second = event(UUID.randomUUID(), "asia", 2, "BLOCK_CHANGE");

        try (SqliteEventJournal journal = new SqliteEventJournal(database)) {
            assertEquals(1, journal.append(first));
            assertEquals(2, journal.append(second));
            assertEquals(2, journal.lastSequence("asia"));
            assertEquals(2, journal.size());
        }

        try (SqliteEventJournal recovered = new SqliteEventJournal(database)) {
            assertEquals(2, recovered.lastSequence("asia"));
            assertEquals(2, recovered.lastOffset());
            var replay = recovered.readAfter(0, 1);
            assertEquals(1, replay.size());
            assertEquals(first, replay.getFirst().envelope());
            assertEquals(1, recovered.readAfter(replay.getFirst().offset(), 10).size());
        }
    }

    @Test
    void exactRetriesReturnTheOriginalOffsetWithoutDuplicatingTheEvent() throws Exception {
        try (SqliteEventJournal journal = new SqliteEventJournal(temporaryDirectory.resolve("events.db"))) {
            ReplicationEnvelope event = event(UUID.randomUUID(), "asia", 1, "BLOCK_CHANGE");
            assertEquals(1, journal.append(event));
            assertEquals(1, journal.append(event));
            assertEquals(1, journal.size());
        }
    }

    @Test
    void rejectsReusingAnOriginSequenceForDifferentEventData() throws Exception {
        try (SqliteEventJournal journal = new SqliteEventJournal(temporaryDirectory.resolve("events.db"))) {
            journal.append(event(UUID.randomUUID(), "asia", 1, "BLOCK_CHANGE"));
            assertThrows(JournalConflictException.class,
                    () -> journal.append(event(UUID.randomUUID(), "asia", 1, "BLOCK_BREAK")));
        }
    }

    @Test
    void rejectsInvalidReplayBounds() throws Exception {
        try (SqliteEventJournal journal = new SqliteEventJournal(temporaryDirectory.resolve("events.db"))) {
            assertThrows(IllegalArgumentException.class, () -> journal.readAfter(-1, 10));
            assertThrows(IllegalArgumentException.class, () -> journal.readAfter(0, 0));
        }
    }

    private static ReplicationEnvelope event(UUID id, String origin, long sequence, String type) {
        return new ReplicationEnvelope(
                ReplicationEnvelope.CURRENT_PROTOCOL_VERSION,
                id,
                "overworld",
                origin,
                sequence,
                Instant.parse("2026-11-13T00:00:00Z"),
                type,
                Map.of("block", "minecraft:stone")
        );
    }
}
