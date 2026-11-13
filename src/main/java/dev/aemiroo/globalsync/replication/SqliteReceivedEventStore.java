package dev.aemiroo.globalsync.replication;

import dev.aemiroo.globalsync.network.ReplicationMessageCodec;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * Durable inbound event inbox and deterministic per-block winner index.
 * World mutation is deliberately performed by a separate Paper-main-thread service.
 */
public final class SqliteReceivedEventStore implements AutoCloseable {
    private final Connection connection;

    public SqliteReceivedEventStore(Path databasePath) throws SQLException {
        try {
            Files.createDirectories(databasePath.toAbsolutePath().getParent());
        } catch (Exception exception) {
            throw new SQLException("Could not create received-event store directory", exception);
        }
        connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath.toAbsolutePath());
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=FULL");
            statement.execute("PRAGMA foreign_keys=ON");
            statement.execute("PRAGMA busy_timeout=5000");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS received_events (
                        receipt_offset INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id TEXT NOT NULL UNIQUE,
                        origin_server_id TEXT NOT NULL,
                        origin_sequence INTEGER NOT NULL CHECK (origin_sequence > 0),
                        logical_clock INTEGER NOT NULL CHECK (logical_clock > 0),
                        world_id TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'PENDING',
                        UNIQUE (origin_server_id, origin_sequence)
                    )
                    """);
            ensureColumn("received_events", "logical_clock", "INTEGER NOT NULL DEFAULT 1");
            // Existing v1 inbox rows were durably received but never applied. Do not
            // reinterpret their old wire payload as a v2 world mutation.
            ensureColumn("received_events", "status", "TEXT NOT NULL DEFAULT 'SUPERSEDED'");
            // Old inbox rows have no trustworthy world-local sequence. Zero is
            // deliberately excluded from snapshot coverage rather than guessed.
            ensureColumn("received_events", "world_sequence", "INTEGER NOT NULL DEFAULT 0");
            statement.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS received_world_origin_sequence
                    ON received_events(world_id, origin_server_id, world_sequence)
                    WHERE world_sequence > 0
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS applied_block_events (
                        event_id TEXT PRIMARY KEY,
                        origin_server_id TEXT NOT NULL,
                        world_sequence INTEGER NOT NULL CHECK (world_sequence > 0),
                        logical_clock INTEGER NOT NULL CHECK (logical_clock > 0),
                        world_id TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        UNIQUE (world_id, origin_server_id, world_sequence)
                    )
                    """);
            statement.execute("""
                    CREATE INDEX IF NOT EXISTS applied_block_events_world_order
                    ON applied_block_events(world_id, logical_clock, origin_server_id, world_sequence)
                    """);
            // Preserve historical effects that are still marked APPLIED. Events
            // superseded before this migration cannot be reconstructed, so the
            // resulting snapshot coverage remains deliberately conservative.
            statement.execute("""
                    INSERT OR IGNORE INTO applied_block_events
                        (event_id, origin_server_id, world_sequence, logical_clock, world_id, payload_json)
                    SELECT event_id, origin_server_id, world_sequence, logical_clock, world_id, payload_json
                    FROM received_events
                    WHERE status = 'APPLIED' AND world_sequence > 0
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS block_winners (
                        world_id TEXT NOT NULL,
                        x INTEGER NOT NULL,
                        y INTEGER NOT NULL,
                        z INTEGER NOT NULL,
                        event_id TEXT NOT NULL,
                        logical_clock INTEGER NOT NULL,
                        origin_server_id TEXT NOT NULL,
                        origin_sequence INTEGER NOT NULL,
                        block_data TEXT NOT NULL,
                        PRIMARY KEY (world_id, x, y, z)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS gap_recovery (
                        origin_server_id TEXT NOT NULL,
                        origin_sequence INTEGER NOT NULL CHECK (origin_sequence > 0),
                        status TEXT NOT NULL CHECK (status IN ('REQUESTED', 'UNAVAILABLE')),
                        first_detected_at INTEGER NOT NULL,
                        last_checked_at INTEGER NOT NULL,
                        unavailable_at INTEGER,
                        PRIMARY KEY (origin_server_id, origin_sequence)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS lamport_state (
                        singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                        clock_value INTEGER NOT NULL CHECK (clock_value >= 0)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS local_journal_state (
                        singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                        indexed_offset INTEGER NOT NULL CHECK (indexed_offset >= 0)
                    )
                    """);
            statement.execute("""
                    INSERT OR IGNORE INTO local_journal_state(singleton_id, indexed_offset)
                    VALUES (1, 0)
                    """);
            statement.execute("""
                    INSERT OR IGNORE INTO lamport_state(singleton_id, clock_value)
                    VALUES (1, 0)
                    """);
            statement.execute("""
                    UPDATE lamport_state SET clock_value = MAX(clock_value,
                        COALESCE((SELECT MAX(logical_clock) FROM received_events), 0))
                    WHERE singleton_id = 1
                    """);
            migrateLegacyInboxRows();
        } catch (SQLException exception) {
            connection.close();
            throw exception;
        }
    }

    /** Returns only after the inbound event and winner decision are durably committed. */
    public synchronized ReceivedEventReceipt accept(ReplicationEnvelope event) throws SQLException {
        return acceptInternal(event, false, 0);
    }

    /**
     * Records a local event after Bukkit has already applied it. It remains
     * pending until the application service confirms that it is still the winner
     * and the live block matches its resulting data.
     */
    public synchronized ReceivedEventReceipt acceptLocal(ReplicationEnvelope event) throws SQLException {
        return acceptInternal(event, true, 0);
    }

    /**
     * Indexes one committed local journal row. The checkpoint and block winner
     * are committed together so startup recovery only scans rows not yet indexed.
     */
    public synchronized void indexLocalJournalEntry(ReplicationEnvelope event, long localOffset)
            throws SQLException {
        if (localOffset < 1) throw new IllegalArgumentException("localOffset must be positive");
        if (BlockChangeEnvelopeFactory.BLOCK_PLACE.equals(event.eventType())
                || BlockChangeEnvelopeFactory.BLOCK_BREAK.equals(event.eventType())) {
            acceptInternal(event, true, localOffset);
            return;
        }
        try {
            connection.setAutoCommit(false);
            updateLocalJournalCheckpoint(localOffset);
            connection.commit();
        } catch (SQLException | RuntimeException exception) {
            try { connection.rollback(); } catch (SQLException rollbackFailure) {
                exception.addSuppressed(rollbackFailure);
            }
            throw exception;
        } finally {
            try { connection.setAutoCommit(true); } catch (SQLException ignored) { }
        }
    }

    public synchronized long lastIndexedLocalJournalOffset() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery(
                     "SELECT indexed_offset FROM local_journal_state WHERE singleton_id = 1")) {
            return result.next() ? result.getLong(1) : 0L;
        }
    }

    private ReceivedEventReceipt acceptInternal(ReplicationEnvelope event, boolean locallyApplied,
                                                  long localOffset) throws SQLException {
        String payload = new String(ReplicationMessageCodec.encodeEvent(event), StandardCharsets.UTF_8);
        try {
            connection.setAutoCommit(false);
            try (PreparedStatement query = connection.prepareStatement("""
                    SELECT receipt_offset, payload_json FROM received_events
                    WHERE event_id = ? OR (origin_server_id = ? AND origin_sequence = ?)
                    """)) {
                query.setString(1, event.eventId().toString());
                query.setString(2, event.originServerId());
                query.setLong(3, event.sequence());
                try (ResultSet result = query.executeQuery()) {
                    Long duplicateOffset = null;
                    while (result.next()) {
                        if (!payload.equals(result.getString("payload_json")) || duplicateOffset != null) {
                            throw new JournalConflictException(
                                    "Received event ID or origin sequence was reused for different data");
                        }
                        duplicateOffset = result.getLong("receipt_offset");
                    }
                    if (duplicateOffset != null) {
                        clearGapRecovery(event.originServerId(), event.sequence());
                        updateLamportClock(event.logicalClock());
                        if (localOffset > 0) updateLocalJournalCheckpoint(localOffset);
                        boolean apply = isPendingWinner(event.eventId());
                        connection.commit();
                        return new ReceivedEventReceipt(duplicateOffset, true, apply);
                    }
                }
            }

            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO received_events
                    (event_id, origin_server_id, origin_sequence, world_sequence, logical_clock, world_id, payload_json, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING')
                    """)) {
                insert.setString(1, event.eventId().toString());
                insert.setString(2, event.originServerId());
                insert.setLong(3, event.sequence());
                insert.setLong(4, event.worldSequence());
                insert.setLong(5, event.logicalClock());
                insert.setString(6, event.worldId());
                insert.setString(7, payload);
                insert.executeUpdate();
            }

            long offset;
            try (Statement query = connection.createStatement();
                 ResultSet result = query.executeQuery("SELECT last_insert_rowid()")) {
                if (!result.next()) throw new SQLException("SQLite did not return received-event offset");
                offset = result.getLong(1);
            }

            clearGapRecovery(event.originServerId(), event.sequence());
            boolean winner = updateWinner(event);
            if (!winner) {
                setStatus(event.eventId(), "SUPERSEDED");
            }
            updateLamportClock(event.logicalClock());
            if (localOffset > 0) updateLocalJournalCheckpoint(localOffset);
            boolean apply = winner;
            if (locallyApplied && !winner) {
                apply = currentWinnerNeedsApplication(event);
            }
            connection.commit();
            return new ReceivedEventReceipt(offset, false, apply);
        } catch (SQLException | RuntimeException exception) {
            try { connection.rollback(); } catch (SQLException rollbackFailure) {
                exception.addSuppressed(rollbackFailure);
            }
            throw exception;
        } finally {
            try { connection.setAutoCommit(true); } catch (SQLException ignored) { }
        }
    }

    /** Highest logical clock committed to this inbox, restored on startup. */
    public synchronized long lastLogicalClock() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery(
                     "SELECT clock_value FROM lamport_state WHERE singleton_id = 1")) {
            return result.next() ? result.getLong(1) : 0L;
        }
    }

    /** Returns pending winners in stable receipt order for startup/retry recovery. */
    public synchronized List<ReplicationEnvelope> pendingWinners() throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT e.payload_json, e.origin_server_id, e.world_id
                FROM received_events e
                JOIN block_winners w ON w.event_id = e.event_id
                WHERE e.status = 'PENDING'
                ORDER BY e.receipt_offset ASC
                """);
             ResultSet result = query.executeQuery()) {
            List<ReplicationEnvelope> events = new ArrayList<>();
            while (result.next()) {
                String world = result.getString("world_id");
                events.add(ReplicationMessageCodec.decodeEvent(
                        result.getString("payload_json").getBytes(StandardCharsets.UTF_8),
                        result.getString("origin_server_id"), Set.of(world)));
            }
            return List.copyOf(events);
        } catch (java.io.IOException exception) {
            throw new SQLException("A stored pending event could not be decoded", exception);
        }
    }

    /** Prevents a queued stale application task from overwriting a newer winner. */
    public synchronized boolean isCurrentWinner(UUID eventId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT 1 FROM block_winners WHERE event_id = ?")) {
            query.setString(1, eventId.toString());
            try (ResultSet result = query.executeQuery()) {
                return result.next();
            }
        }
    }


    /**
     * Runs the Paper-thread mutation while holding the inbox lock, so a newer
     * winner cannot be committed between the winner check and the world write.
     * If the callback fails, the event remains pending for retry after restart.
     */
    public synchronized boolean applyCurrentWinner(UUID eventId, BooleanSupplier worldMutation) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT 1 FROM received_events e
                JOIN block_winners w ON w.event_id = e.event_id
                WHERE e.event_id = ? AND e.status = 'PENDING'
                """)) {
            query.setString(1, eventId.toString());
            try (ResultSet result = query.executeQuery()) {
                if (!result.next()) return false;
            }
        }
        if (!worldMutation.getAsBoolean()) return false;

        // Commit the applied status and its immutable reconciliation record as
        // one transaction. If persistence fails after the world mutation, the
        // event stays pending; retry observes the resulting block value and
        // records the effect before claiming it applied.
        try {
            connection.setAutoCommit(false);
            try (PreparedStatement history = connection.prepareStatement("""
                    INSERT OR IGNORE INTO applied_block_events
                        (event_id, origin_server_id, world_sequence, logical_clock, world_id, payload_json)
                    SELECT event_id, origin_server_id, world_sequence, logical_clock, world_id, payload_json
                    FROM received_events
                    WHERE event_id = ? AND world_sequence > 0
                    """)) {
                history.setString(1, eventId.toString());
                history.executeUpdate();
            }
            int updated;
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE received_events SET status = 'APPLIED' WHERE event_id = ? AND status = 'PENDING'")) {
                update.setString(1, eventId.toString());
                updated = update.executeUpdate();
            }
            connection.commit();
            return updated == 1;
        } catch (SQLException | RuntimeException exception) {
            try { connection.rollback(); } catch (SQLException rollbackFailure) {
                exception.addSuppressed(rollbackFailure);
            }
            throw exception;
        } finally {
            try { connection.setAutoCommit(true); } catch (SQLException ignored) { }
        }
    }


    /**
     * Returns durable block effects successfully applied after a snapshot's
     * conservative per-origin boundary. Records are retained even if a later
     * event supersedes their inbox status.
     */
    public synchronized List<ReplicationEnvelope> appliedMutationsAfter(
            String worldId, Map<String, Long> appliedThrough) throws SQLException {
        if (worldId == null || worldId.isBlank()) throw new IllegalArgumentException("worldId is required");
        if (appliedThrough == null || appliedThrough.size() > SnapshotManifest.MAX_ORIGINS)
            throw new IllegalArgumentException("Invalid snapshot applied-through vector");
        appliedThrough.forEach((origin, sequence) -> {
            if (origin == null || origin.isBlank() || sequence == null || sequence < 0)
                throw new IllegalArgumentException("Invalid snapshot applied-through entry");
        });

        List<ReplicationEnvelope> events = new ArrayList<>();
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT payload_json, origin_server_id, world_sequence
                FROM applied_block_events
                WHERE world_id = ?
                ORDER BY logical_clock ASC, origin_server_id ASC, world_sequence ASC
                """)) {
            query.setString(1, worldId);
            try (ResultSet rows = query.executeQuery()) {
                while (rows.next()) {
                    String origin = rows.getString("origin_server_id");
                    long worldSequence = rows.getLong("world_sequence");
                    if (worldSequence <= appliedThrough.getOrDefault(origin, 0L)) continue;
                    events.add(ReplicationMessageCodec.decodeEvent(
                            rows.getString("payload_json").getBytes(StandardCharsets.UTF_8),
                            origin, Set.of(worldId)));
                }
            }
        } catch (java.io.IOException exception) {
            throw new SQLException("An applied block event could not be decoded", exception);
        }
        return List.copyOf(events);
    }

    /**
     * Returns contiguous, durably applied world-local sequence coverage for
     * each (world, origin) pair. Legacy rows with world_sequence=0 and events
     * that are merely received or superseded are never counted as applied.
     *
     * A zero contiguous sequence means no positive world-local event from that
     * origin is yet proven applied in that world.
     */
    public synchronized List<WorldAppliedSequenceProgress> worldAppliedSequenceProgress()
            throws SQLException {
        List<WorldAppliedSequenceProgress> progress = new ArrayList<>();
        try (Statement origins = connection.createStatement();
             ResultSet groups = origins.executeQuery("""
                     SELECT world_id, origin_server_id, COUNT(*) AS applied_count,
                            MAX(world_sequence) AS highest_sequence
                     FROM applied_block_events
                     WHERE world_sequence > 0
                     GROUP BY world_id, origin_server_id
                     ORDER BY world_id, origin_server_id
                     """)) {
            while (groups.next()) {
                String world = groups.getString("world_id");
                String origin = groups.getString("origin_server_id");
                long count = groups.getLong("applied_count");
                long highest = groups.getLong("highest_sequence");
                long contiguous = 0L;
                if (count >= highest) {
                    contiguous = highest;
                } else {
                    long expected = 1L;
                    try (PreparedStatement sequences = connection.prepareStatement("""
                            SELECT world_sequence FROM applied_block_events
                            WHERE world_id = ? AND origin_server_id = ?
                              AND world_sequence > 0
                            ORDER BY world_sequence ASC
                            """)) {
                        sequences.setString(1, world);
                        sequences.setString(2, origin);
                        try (ResultSet rows = sequences.executeQuery()) {
                            while (rows.next()) {
                                long sequence = rows.getLong(1);
                                if (sequence < expected) continue;
                                if (sequence != expected) break;
                                contiguous = expected;
                                expected++;
                            }
                        }
                    }
                }
                progress.add(new WorldAppliedSequenceProgress(
                        world, origin, contiguous, highest, count));
            }
        }
        return List.copyOf(progress);
    }

    public synchronized long size() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery("SELECT COUNT(*) FROM received_events")) {
            return result.next() ? result.getLong(1) : 0;
        }
    }

    /** Records that this node has checked/requested one exact missing origin sequence. */
    public synchronized void markGapRequested(String originServerId, long sequence, Instant checkedAt)
            throws SQLException {
        validateGapKey(originServerId, sequence);
        long checked = java.util.Objects.requireNonNull(checkedAt, "checkedAt").toEpochMilli();
        try (PreparedStatement upsert = connection.prepareStatement("""
                INSERT INTO gap_recovery
                    (origin_server_id, origin_sequence, status, first_detected_at, last_checked_at, unavailable_at)
                VALUES (?, ?, 'REQUESTED', ?, ?, NULL)
                ON CONFLICT(origin_server_id, origin_sequence) DO UPDATE SET
                    status = CASE WHEN gap_recovery.status = 'UNAVAILABLE'
                                  THEN 'UNAVAILABLE' ELSE 'REQUESTED' END,
                    last_checked_at = excluded.last_checked_at
                """)) {
            upsert.setString(1, originServerId);
            upsert.setLong(2, sequence);
            upsert.setLong(3, checked);
            upsert.setLong(4, checked);
            upsert.executeUpdate();
        }
    }

    /** Persists the origin's definitive NOT_RETAINED response for one exact gap. */
    public synchronized void markGapUnavailable(String originServerId, long sequence, Instant unavailableAt)
            throws SQLException {
        validateGapKey(originServerId, sequence);
        long unavailable = java.util.Objects.requireNonNull(unavailableAt, "unavailableAt").toEpochMilli();
        try (PreparedStatement upsert = connection.prepareStatement("""
                INSERT INTO gap_recovery
                    (origin_server_id, origin_sequence, status, first_detected_at, last_checked_at, unavailable_at)
                VALUES (?, ?, 'UNAVAILABLE', ?, ?, ?)
                ON CONFLICT(origin_server_id, origin_sequence) DO UPDATE SET
                    status = 'UNAVAILABLE',
                    last_checked_at = excluded.last_checked_at,
                    unavailable_at = excluded.unavailable_at
                """)) {
            upsert.setString(1, originServerId);
            upsert.setLong(2, sequence);
            upsert.setLong(3, unavailable);
            upsert.setLong(4, unavailable);
            upsert.setLong(5, unavailable);
            upsert.executeUpdate();
        }
    }

    /** Returns every unresolved exact gap-recovery record in stable order. */
    public synchronized List<GapRecoveryProgress> gapRecoveryProgress() throws SQLException {
        List<GapRecoveryProgress> progress = new ArrayList<>();
        try (Statement query = connection.createStatement();
             ResultSet rows = query.executeQuery("""
                     SELECT origin_server_id, origin_sequence, status,
                            first_detected_at, last_checked_at, unavailable_at
                     FROM gap_recovery
                     ORDER BY origin_server_id, origin_sequence
                     """)) {
            while (rows.next()) {
                long unavailable = rows.getLong("unavailable_at");
                Long unavailableAt = rows.wasNull() ? null : unavailable;
                progress.add(new GapRecoveryProgress(
                        rows.getString("origin_server_id"),
                        rows.getLong("origin_sequence"),
                        rows.getString("status"),
                        Instant.ofEpochMilli(rows.getLong("first_detected_at")),
                        Instant.ofEpochMilli(rows.getLong("last_checked_at")),
                        unavailableAt == null ? null : Instant.ofEpochMilli(unavailableAt)));
            }
        }
        return List.copyOf(progress);
    }

    private void clearGapRecovery(String originServerId, long sequence) throws SQLException {
        try (PreparedStatement delete = connection.prepareStatement("""
                DELETE FROM gap_recovery WHERE origin_server_id = ? AND origin_sequence = ?
                """)) {
            delete.setString(1, originServerId);
            delete.setLong(2, sequence);
            delete.executeUpdate();
        }
    }

    private static void validateGapKey(String originServerId, long sequence) {
        if (originServerId == null || !originServerId.matches("[A-Za-z0-9._-]{1,64}"))
            throw new IllegalArgumentException("Invalid gap origin server ID");
        if (sequence < 1) throw new IllegalArgumentException("Gap sequence must be positive");
    }

    /**
     * Reports the first missing origin sequence for every origin with received
     * events. A zero firstMissingSequence means no hole exists through the
     * highest observed sequence. The query is read-only and does not alter ACKs.
     */
    public synchronized List<OriginSequenceProgress> originSequenceProgress() throws SQLException {
        List<OriginSequenceProgress> progress = new ArrayList<>();
        try (Statement origins = connection.createStatement();
             ResultSet result = origins.executeQuery("""
                     SELECT origin_server_id, COUNT(*) AS received_count,
                            MAX(origin_sequence) AS highest_sequence
                     FROM received_events
                     GROUP BY origin_server_id
                     ORDER BY origin_server_id
                     """)) {
            while (result.next()) {
                String origin = result.getString("origin_server_id");
                long received = result.getLong("received_count");
                long highest = result.getLong("highest_sequence");
                long firstMissing = 0L;
                // With positive unique sequences, count == max proves the full
                // range 1..max is present without scanning every row.
                if (received < highest) {
                    long expected = 1L;
                    try (PreparedStatement sequences = connection.prepareStatement("""
                            SELECT origin_sequence FROM received_events
                            WHERE origin_server_id = ?
                            ORDER BY origin_sequence ASC
                            """)) {
                        sequences.setString(1, origin);
                        try (ResultSet sequenceRows = sequences.executeQuery()) {
                            while (sequenceRows.next()) {
                                long sequence = sequenceRows.getLong(1);
                                if (sequence > expected) {
                                    firstMissing = expected;
                                    break;
                                }
                                if (sequence == expected) expected++;
                            }
                        }
                    }
                    if (firstMissing == 0L && expected <= highest) firstMissing = expected;
                }
                progress.add(new OriginSequenceProgress(origin, received, highest, firstMissing));
            }
        }
        return List.copyOf(progress);
    }


    private void migrateLegacyInboxRows() throws SQLException {
        List<ReplicationEnvelope> legacyEvents = new ArrayList<>();
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT payload_json FROM received_events
                WHERE payload_json LIKE '%"protocolVersion":1%'
                """);
             ResultSet result = query.executeQuery()) {
            while (result.next()) {
                try {
                    legacyEvents.add(ReplicationMessageCodec.decodeLegacyEvent(
                            result.getString("payload_json").getBytes(StandardCharsets.UTF_8)));
                } catch (java.io.IOException exception) {
                    throw new SQLException("A legacy inbox event could not be migrated safely", exception);
                }
            }
        }
        if (legacyEvents.isEmpty()) return;

        legacyEvents.sort(Comparator.comparingLong(ReplicationEnvelope::logicalClock)
                .thenComparing(ReplicationEnvelope::originServerId)
                .thenComparingLong(ReplicationEnvelope::sequence));

        try {
            connection.setAutoCommit(false);
            for (ReplicationEnvelope event : legacyEvents) {
                String payload = new String(ReplicationMessageCodec.encodeEvent(event), StandardCharsets.UTF_8);
                try (PreparedStatement update = connection.prepareStatement("""
                        UPDATE received_events
                        SET logical_clock = ?, payload_json = ?, status = 'PENDING'
                        WHERE event_id = ?
                        """)) {
                    update.setLong(1, event.logicalClock());
                    update.setString(2, payload);
                    update.setString(3, event.eventId().toString());
                    update.executeUpdate();
                }
                if (!updateWinner(event)) setStatus(event.eventId(), "SUPERSEDED");
                updateLamportClock(event.logicalClock());
            }
            connection.commit();
        } catch (SQLException | RuntimeException exception) {
            try { connection.rollback(); } catch (SQLException rollbackFailure) {
                exception.addSuppressed(rollbackFailure);
            }
            throw exception;
        } finally {
            try { connection.setAutoCommit(true); } catch (SQLException ignored) { }
        }
    }

    private boolean updateWinner(ReplicationEnvelope event) throws SQLException {
        int x = Integer.parseInt(event.attributes().get("x"));
        int y = Integer.parseInt(event.attributes().get("y"));
        int z = Integer.parseInt(event.attributes().get("z"));
        String previousId = null;
        long previousClock = -1;
        String previousOrigin = null;
        long previousSequence = -1;
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT event_id, logical_clock, origin_server_id, origin_sequence
                FROM block_winners WHERE world_id = ? AND x = ? AND y = ? AND z = ?
                """)) {
            query.setString(1, event.worldId());
            query.setInt(2, x);
            query.setInt(3, y);
            query.setInt(4, z);
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    previousId = result.getString("event_id");
                    previousClock = result.getLong("logical_clock");
                    previousOrigin = result.getString("origin_server_id");
                    previousSequence = result.getLong("origin_sequence");
                }
            }
        }

        if (previousId != null && compareOrder(event.logicalClock(), event.originServerId(),
                event.sequence(), previousClock, previousOrigin, previousSequence) <= 0) {
            return previousId.equals(event.eventId().toString());
        }

        if (previousId != null) setStatus(UUID.fromString(previousId), "SUPERSEDED");
        try (PreparedStatement upsert = connection.prepareStatement("""
                INSERT INTO block_winners
                    (world_id, x, y, z, event_id, logical_clock, origin_server_id, origin_sequence, block_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(world_id, x, y, z) DO UPDATE SET
                    event_id = excluded.event_id,
                    logical_clock = excluded.logical_clock,
                    origin_server_id = excluded.origin_server_id,
                    origin_sequence = excluded.origin_sequence,
                    block_data = excluded.block_data
                """)) {
            upsert.setString(1, event.worldId());
            upsert.setInt(2, x);
            upsert.setInt(3, y);
            upsert.setInt(4, z);
            upsert.setString(5, event.eventId().toString());
            upsert.setLong(6, event.logicalClock());
            upsert.setString(7, event.originServerId());
            upsert.setLong(8, event.sequence());
            upsert.setString(9, event.attributes().get("new_block_data"));
            upsert.executeUpdate();
        }
        return true;
    }

    private static int compareOrder(long clockA, String originA, long sequenceA,
                                    long clockB, String originB, long sequenceB) {
        int clock = Long.compare(clockA, clockB);
        if (clock != 0) return clock;
        int origin = originA.compareTo(originB);
        if (origin != 0) return origin;
        return Long.compare(sequenceA, sequenceB);
    }

    private boolean currentWinnerNeedsApplication(ReplicationEnvelope event) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT e.status FROM block_winners w
                JOIN received_events e ON e.event_id = w.event_id
                WHERE w.world_id = ? AND w.x = ? AND w.y = ? AND w.z = ?
                """)) {
            query.setString(1, event.worldId());
            query.setInt(2, Integer.parseInt(event.attributes().get("x")));
            query.setInt(3, Integer.parseInt(event.attributes().get("y")));
            query.setInt(4, Integer.parseInt(event.attributes().get("z")));
            try (ResultSet result = query.executeQuery()) {
                return result.next() && "PENDING".equals(result.getString(1));
            }
        }
    }

    private boolean isPendingWinner(UUID eventId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT 1 FROM received_events e
                JOIN block_winners w ON w.event_id = e.event_id
                WHERE e.event_id = ? AND e.status = 'PENDING'
                """)) {
            query.setString(1, eventId.toString());
            try (ResultSet result = query.executeQuery()) {
                return result.next();
            }
        }
    }

    private void setStatus(UUID eventId, String status) throws SQLException {
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE received_events SET status = ? WHERE event_id = ?")) {
            update.setString(1, status);
            update.setString(2, eventId.toString());
            update.executeUpdate();
        }
    }


    private void updateLocalJournalCheckpoint(long localOffset) throws SQLException {
        long current = lastIndexedLocalJournalOffset();
        if (localOffset < current) {
            throw new SQLException("Local journal index checkpoint cannot move backwards");
        }
        if (localOffset == current) return;
        try (PreparedStatement update = connection.prepareStatement("""
                UPDATE local_journal_state SET indexed_offset = ?
                WHERE singleton_id = 1
                """)) {
            update.setLong(1, localOffset);
            update.executeUpdate();
        }
    }

    private void updateLamportClock(long remoteClock) throws SQLException {
        try (PreparedStatement update = connection.prepareStatement("""
                UPDATE lamport_state SET clock_value = MAX(clock_value, ?)
                WHERE singleton_id = 1
                """)) {
            update.setLong(1, remoteClock);
            update.executeUpdate();
        }
    }

    private void ensureColumn(String table, String column, String declaration) throws SQLException {
        boolean present = false;
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery("PRAGMA table_info(" + table + ")")) {
            while (result.next()) {
                if (column.equalsIgnoreCase(result.getString("name"))) {
                    present = true;
                    break;
                }
            }
        }
        if (!present) {
            try (Statement alter = connection.createStatement()) {
                alter.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + declaration);
            }
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        connection.close();
    }
}
