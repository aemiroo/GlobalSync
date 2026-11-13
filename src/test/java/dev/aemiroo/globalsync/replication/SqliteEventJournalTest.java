package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
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
            assertEquals(2, journal.lastLogicalClock());
            assertEquals(2, journal.size());
        }

        try (SqliteEventJournal recovered = new SqliteEventJournal(database)) {
            assertEquals(2, recovered.lastSequence("asia"));
            assertEquals(2, recovered.lastLogicalClock());
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
    void reportsPerPeerBacklogAndOldestPendingEventWithoutUsingRemoteOffsetsAsLocalCursors() throws Exception {
        try (SqliteEventJournal journal = new SqliteEventJournal(temporaryDirectory.resolve("progress/events.db"))) {
            ReplicationEnvelope first = event(UUID.randomUUID(), "asia", 1, "BLOCK_PLACE");
            ReplicationEnvelope second = event(UUID.randomUUID(), "asia", 2, "BLOCK_BREAK");
            long firstOffset = journal.append(first);
            long secondOffset = journal.append(second);

            PeerReplicationProgress disconnected = journal.peerProgress("europe", false, 0);
            assertEquals("DISCONNECTED", disconnected.state());
            assertEquals(2, disconnected.pendingEvents());
            assertEquals(0, disconnected.acknowledgedOffset());
            assertEquals(2, disconnected.journalHeadOffset());
            assertEquals(first.createdAt(), disconnected.oldestPendingAt());

            journal.acknowledge("europe", new JournalEntry(firstOffset, first), 50);
            PeerReplicationProgress catchingUp = journal.peerProgress("europe", true, 3);
            assertEquals("CATCHING_UP", catchingUp.state());
            assertEquals(1, catchingUp.pendingEvents());
            assertEquals(firstOffset, catchingUp.acknowledgedOffset());
            assertEquals(secondOffset, catchingUp.journalHeadOffset());
            assertEquals(3, catchingUp.heartbeatPongs());

            journal.acknowledge("europe", new JournalEntry(secondOffset, second), 51);
            PeerReplicationProgress current = journal.peerProgress("europe", true, 4);
            assertEquals("CURRENT", current.state());
            assertEquals(0, current.pendingEvents());
            assertNull(current.oldestPendingAt());
        }
    }

    @Test
    void findsExactOriginSequenceWithoutConfusingOtherOriginsOrLocalOffsets() throws Exception {
        Path database = temporaryDirectory.resolve("origin-lookup/events.db");
        ReplicationEnvelope asiaFirst = event(UUID.randomUUID(), "asia", 1, "BLOCK_PLACE");
        ReplicationEnvelope europeFirst = event(UUID.randomUUID(), "europe", 1, "BLOCK_BREAK");
        ReplicationEnvelope asiaSecond = event(UUID.randomUUID(), "asia", 2, "BLOCK_BREAK");

        try (SqliteEventJournal journal = new SqliteEventJournal(database)) {
            long firstOffset = journal.append(asiaFirst);
            journal.append(europeFirst);
            long secondOffset = journal.append(asiaSecond);

            var found = journal.findByOriginSequence("asia", 2);
            assertTrue(found.isPresent());
            assertEquals(secondOffset, found.orElseThrow().offset());
            assertEquals(asiaSecond, found.orElseThrow().envelope());

            assertEquals(asiaFirst, journal.findByOriginSequence("asia", 1).orElseThrow().envelope());
            assertEquals(europeFirst, journal.findByOriginSequence("europe", 1).orElseThrow().envelope());
            assertTrue(journal.findByOriginSequence("asia", 3).isEmpty());
            assertEquals(firstOffset, journal.findByOriginSequence("asia", 1).orElseThrow().offset());
        }

        try (SqliteEventJournal recovered = new SqliteEventJournal(database)) {
            assertEquals(asiaSecond,
                    recovered.findByOriginSequence("asia", 2).orElseThrow().envelope());
        }
    }

    @Test
    void rejectsInvalidOriginSequenceLookupArguments() throws Exception {
        try (SqliteEventJournal journal = new SqliteEventJournal(temporaryDirectory.resolve("invalid-lookup.db"))) {
            assertThrows(IllegalArgumentException.class, () -> journal.findByOriginSequence("../asia", 1));
            assertThrows(IllegalArgumentException.class, () -> journal.findByOriginSequence("asia", 0));
        }
    }

    @Test
    void rejectsInvalidReplayBounds() throws Exception {
        try (SqliteEventJournal journal = new SqliteEventJournal(temporaryDirectory.resolve("events.db"))) {
            assertThrows(IllegalArgumentException.class, () -> journal.readAfter(-1, 10));
            assertThrows(IllegalArgumentException.class, () -> journal.readAfter(0, 0));
        }
    }

    @Test
    void persistsPerPeerAcknowledgementCursorAndRejectsSkippedEntries() throws Exception {
        Path database = temporaryDirectory.resolve("delivery/events.db");
        ReplicationEnvelope first = event(UUID.randomUUID(), "asia", 1, "BLOCK_PLACE");
        ReplicationEnvelope second = event(UUID.randomUUID(), "asia", 2, "BLOCK_BREAK");
        try (SqliteEventJournal journal = new SqliteEventJournal(database)) {
            long firstOffset = journal.append(first);
            long secondOffset = journal.append(second);
            assertEquals(0, journal.acknowledgedOffset("europe"));
            assertThrows(SQLException.class,
                    () -> journal.acknowledge("europe", new JournalEntry(secondOffset, second), 2));
            journal.acknowledge("europe", new JournalEntry(firstOffset, first), 7);
            assertEquals(firstOffset, journal.acknowledgedOffset("europe"));
            assertEquals(7, journal.remoteReceiptOffset("europe"));
            journal.acknowledge("europe", new JournalEntry(secondOffset, second), 8);
            assertEquals(secondOffset, journal.acknowledgedOffset("europe"));
            assertThrows(SQLException.class,
                    () -> journal.acknowledge("europe", new JournalEntry(firstOffset, first), 7));
        }
        try (SqliteEventJournal recovered = new SqliteEventJournal(database)) {
            assertEquals(2, recovered.acknowledgedOffset("europe"));
            assertEquals(8, recovered.remoteReceiptOffset("europe"));
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

    @Test
    void restoresWorldSequencesIndependentlyFromTheGlobalOriginSequence() throws Exception {
        Path database = temporaryDirectory.resolve("world-sequences/events.db");
        ReplicationEnvelope overworld = new ReplicationEnvelope(3, UUID.randomUUID(), "overworld", "asia",
                10, 4, 10, Instant.now(), "BLOCK_PLACE", Map.of("block", "minecraft:stone"));
        ReplicationEnvelope nether = new ReplicationEnvelope(3, UUID.randomUUID(), "nether", "asia",
                11, 2, 11, Instant.now(), "BLOCK_PLACE", Map.of("block", "minecraft:netherrack"));

        try (SqliteEventJournal journal = new SqliteEventJournal(database)) {
            journal.append(overworld);
            journal.append(nether);
            assertEquals(4, journal.lastWorldSequence("asia", "overworld"));
            assertEquals(2, journal.lastWorldSequence("asia", "nether"));
        }
        try (SqliteEventJournal recovered = new SqliteEventJournal(database)) {
            assertEquals(4, recovered.lastWorldSequence("asia", "overworld"));
            assertEquals(2, recovered.lastWorldSequence("asia", "nether"));
        }
    }
}
