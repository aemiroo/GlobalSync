package dev.aemiroo.globalsync.replication;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Append-only, crash-recoverable local event journal.
 *
 * <p>SQLite is configured for WAL mode and FULL synchronous commits. Each append
 * is committed before its offset is returned. The journal is local to one node;
 * it does not establish a global order or replicate events by itself.</p>
 */
public final class SqliteEventJournal implements AutoCloseable {
    private static final Type ATTRIBUTES_TYPE = new TypeToken<Map<String, String>>() { }.getType();

    private final Connection connection;
    private final Gson gson = new Gson();

    public SqliteEventJournal(Path databasePath) throws SQLException {
        try {
            Files.createDirectories(databasePath.toAbsolutePath().getParent());
        } catch (Exception exception) {
            throw new SQLException("Could not create the journal directory", exception);
        }

        this.connection = DriverManager.getConnection("jdbc:sqlite:" + databasePath.toAbsolutePath());
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=FULL");
            statement.execute("PRAGMA foreign_keys=ON");
            statement.execute("PRAGMA busy_timeout=5000");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS events (
                        journal_offset INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id TEXT NOT NULL UNIQUE,
                        world_id TEXT NOT NULL,
                        origin_server_id TEXT NOT NULL,
                        origin_sequence INTEGER NOT NULL CHECK (origin_sequence > 0),
                        world_sequence INTEGER NOT NULL DEFAULT 0 CHECK (world_sequence >= 0),
                        logical_clock INTEGER NOT NULL CHECK (logical_clock > 0),
                        protocol_version INTEGER NOT NULL,
                        created_at TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        attributes_json TEXT NOT NULL,
                        UNIQUE (origin_server_id, origin_sequence)
                    )
                    """);
            statement.execute("""
                    CREATE INDEX IF NOT EXISTS idx_events_world_offset
                    ON events(world_id, journal_offset)
                    """);
            ensureColumn("events", "logical_clock", "INTEGER NOT NULL DEFAULT 1");
            ensureColumn("events", "world_sequence", "INTEGER NOT NULL DEFAULT 0");
            statement.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_events_origin_world_sequence
                    ON events(origin_server_id, world_id, world_sequence)
                    WHERE world_sequence > 0
                    """);
            // Upgrade previously journaled v1 events to the v2 envelope. Their
            // per-node sequence is the only safe legacy logical-order seed.
            statement.execute("UPDATE events SET logical_clock = MIN(origin_sequence, 1000000000) WHERE protocol_version = 1");
            // Existing rows retain their global sequence identity. A zero world sequence
            // marks legacy history that predates world-scoped cursors.
            statement.execute("UPDATE events SET protocol_version = 3, world_sequence = 0 WHERE protocol_version < 3");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS peer_delivery (
                        peer_server_id TEXT PRIMARY KEY,
                        acknowledged_offset INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged_offset >= 0),
                        last_event_id TEXT,
                        remote_receipt_offset INTEGER NOT NULL DEFAULT 0 CHECK (remote_receipt_offset >= 0),
                        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """);
        } catch (SQLException exception) {
            connection.close();
            throw exception;
        }
    }

    /**
     * Appends an event and returns its local durable offset.
     * Re-appending the exact same event is idempotent and returns the original offset.
     */
    public synchronized long append(ReplicationEnvelope envelope) throws SQLException {
        String sql = """
                INSERT INTO events (
                    event_id, world_id, origin_server_id, origin_sequence, world_sequence, logical_clock,
                    protocol_version, created_at, event_type, attributes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try (PreparedStatement insert = connection.prepareStatement(sql)) {
            insert.setString(1, envelope.eventId().toString());
            insert.setString(2, envelope.worldId());
            insert.setString(3, envelope.originServerId());
            insert.setLong(4, envelope.sequence());
            insert.setLong(5, envelope.worldSequence());
            insert.setLong(6, envelope.logicalClock());
            insert.setInt(7, envelope.protocolVersion());
            insert.setString(8, envelope.createdAt().toString());
            insert.setString(9, envelope.eventType());
            insert.setString(10, gson.toJson(envelope.attributes()));
            insert.executeUpdate();
            return lastInsertedOffset();
        } catch (SQLException insertFailure) {
            JournalEntry existing = findConflict(envelope);
            if (existing != null) {
                if (existing.envelope().equals(envelope)) {
                    return existing.offset();
                }
                throw new JournalConflictException(
                        "Event ID or origin sequence was already used for different event data");
            }
            throw insertFailure;
        }
    }

    public synchronized List<JournalEntry> readAfter(long offset, int limit) throws SQLException {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive");
        }

        try (PreparedStatement query = connection.prepareStatement("""
                SELECT journal_offset, event_id, world_id, origin_server_id, origin_sequence, world_sequence,
                       logical_clock, protocol_version, created_at, event_type, attributes_json
                FROM events
                WHERE journal_offset > ?
                ORDER BY journal_offset ASC
                LIMIT ?
                """)) {
            query.setLong(1, offset);
            query.setInt(2, limit);
            try (ResultSet result = query.executeQuery()) {
                var entries = new java.util.ArrayList<JournalEntry>();
                while (result.next()) {
                    entries.add(readEntry(result));
                }
                return List.copyOf(entries);
            }
        }
    }

    /** Returns this peer's durable local-journal acknowledgement cursor. */
    public synchronized long acknowledgedOffset(String peerServerId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT acknowledged_offset FROM peer_delivery WHERE peer_server_id = ?")) {
            query.setString(1, peerServerId);
            try (ResultSet result = query.executeQuery()) {
                return result.next() ? result.getLong(1) : 0L;
            }
        }
    }

    /**
     * Advances a peer cursor only for the exact next local journal entry.
     * The remote receipt offset is diagnostic metadata; it is never used as a
     * cursor in this node's journal.
     */
    public synchronized void acknowledge(String peerServerId, JournalEntry entry,
                                         long remoteReceiptOffset) throws SQLException {
        if (peerServerId == null || !peerServerId.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("Invalid peer server ID");
        }
        if (remoteReceiptOffset < 1) throw new IllegalArgumentException("remoteReceiptOffset must be positive");

        try (PreparedStatement query = connection.prepareStatement(
                "SELECT event_id FROM events WHERE journal_offset = ?")) {
            query.setLong(1, entry.offset());
            try (ResultSet result = query.executeQuery()) {
                if (!result.next() || !entry.envelope().eventId().toString().equals(result.getString(1))) {
                    throw new SQLException("Acknowledgement does not match a local journal entry");
                }
            }
        }

        long current = acknowledgedOffset(peerServerId);
        if (entry.offset() < current) throw new SQLException("Peer acknowledgement cursor cannot move backwards");
        if (entry.offset() == current) {
            try (PreparedStatement query = connection.prepareStatement(
                    "SELECT last_event_id FROM peer_delivery WHERE peer_server_id = ?")) {
                query.setString(1, peerServerId);
                try (ResultSet result = query.executeQuery()) {
                    if (result.next() && entry.envelope().eventId().toString().equals(result.getString(1))) return;
                }
            }
            throw new SQLException("Acknowledgement conflicts with the persisted peer cursor");
        }

        try (PreparedStatement query = connection.prepareStatement(
                "SELECT MIN(journal_offset) FROM events WHERE journal_offset > ?")) {
            query.setLong(1, current);
            try (ResultSet result = query.executeQuery()) {
                if (!result.next() || result.getLong(1) != entry.offset()) {
                    throw new SQLException("Peer acknowledgement would skip an unacknowledged journal entry");
                }
            }
        }

        long previousRemoteReceipt = remoteReceiptOffset(peerServerId);
        if (remoteReceiptOffset <= previousRemoteReceipt) {
            throw new SQLException("Receiver receipt offset did not advance");
        }

        try (PreparedStatement update = connection.prepareStatement("""
                INSERT INTO peer_delivery
                    (peer_server_id, acknowledged_offset, last_event_id, remote_receipt_offset, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(peer_server_id) DO UPDATE SET
                    acknowledged_offset = excluded.acknowledged_offset,
                    last_event_id = excluded.last_event_id,
                    remote_receipt_offset = excluded.remote_receipt_offset,
                    updated_at = CURRENT_TIMESTAMP
                """)) {
            update.setString(1, peerServerId);
            update.setLong(2, entry.offset());
            update.setString(3, entry.envelope().eventId().toString());
            update.setLong(4, remoteReceiptOffset);
            update.executeUpdate();
        }
    }

    public synchronized long remoteReceiptOffset(String peerServerId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT remote_receipt_offset FROM peer_delivery WHERE peer_server_id = ?")) {
            query.setString(1, peerServerId);
            try (ResultSet result = query.executeQuery()) {
                return result.next() ? result.getLong(1) : 0L;
            }
        }
    }

    /**
     * Returns a local-only progress snapshot for one configured peer. The
     * acknowledged offset is this node's cursor, never the receiver's offset.
     */
    public synchronized PeerReplicationProgress peerProgress(String peerServerId,
                                                               boolean connected,
                                                               long heartbeatPongs) throws SQLException {
        if (peerServerId == null || !peerServerId.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("Invalid peer server ID");
        }
        if (heartbeatPongs < 0) throw new IllegalArgumentException("heartbeatPongs must not be negative");

        long head = lastOffset();
        long acknowledged = acknowledgedOffset(peerServerId);
        long pending;
        Instant oldest = null;
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT COUNT(*), MIN(created_at)
                FROM events
                WHERE journal_offset > ?
                """)) {
            query.setLong(1, acknowledged);
            try (ResultSet result = query.executeQuery()) {
                if (result.next()) {
                    pending = result.getLong(1);
                    String timestamp = result.getString(2);
                    if (timestamp != null) oldest = Instant.parse(timestamp);
                } else {
                    pending = 0;
                }
            }
        }
        return new PeerReplicationProgress(peerServerId, connected, heartbeatPongs,
                head, acknowledged, pending, oldest);
    }

    public synchronized long lastOffset() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery("SELECT COALESCE(MAX(journal_offset), 0) FROM events")) {
            return result.next() ? result.getLong(1) : 0;
        }
    }

    /**
     * Finds one exact event in this node's durable journal by its origin-local
     * identity. This does not authorize a network requester; callers must bind
     * the requested origin to the authenticated peer before using the result.
     */
    public synchronized Optional<JournalEntry> findByOriginSequence(String originServerId,
                                                                      long originSequence) throws SQLException {
        if (originServerId == null || !originServerId.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("Invalid origin server ID");
        }
        if (originSequence < 1) {
            throw new IllegalArgumentException("originSequence must be positive");
        }

        try (PreparedStatement query = connection.prepareStatement("""
                SELECT journal_offset, event_id, world_id, origin_server_id, origin_sequence, world_sequence,
                       logical_clock, protocol_version, created_at, event_type, attributes_json
                FROM events
                WHERE origin_server_id = ? AND origin_sequence = ?
                """)) {
            query.setString(1, originServerId);
            query.setLong(2, originSequence);
            try (ResultSet result = query.executeQuery()) {
                return result.next() ? Optional.of(readEntry(result)) : Optional.empty();
            }
        }
    }

    public synchronized long lastSequence(String originServerId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT COALESCE(MAX(origin_sequence), 0) FROM events WHERE origin_server_id = ?")) {
            query.setString(1, originServerId);
            try (ResultSet result = query.executeQuery()) {
                return result.next() ? result.getLong(1) : 0;
            }
        }
    }


    /** Highest world-local sequence persisted for this origin and world. */
    public synchronized long lastWorldSequence(String originServerId, String worldId) throws SQLException {
        if (originServerId == null || !originServerId.matches("[A-Za-z0-9._-]{1,64}"))
            throw new IllegalArgumentException("Invalid origin server ID");
        if (worldId == null || worldId.isBlank())
            throw new IllegalArgumentException("Invalid world ID");
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT COALESCE(MAX(world_sequence), 0) FROM events WHERE origin_server_id = ? AND world_id = ?")) {
            query.setString(1, originServerId);
            query.setString(2, worldId);
            try (ResultSet result = query.executeQuery()) {
                return result.next() ? result.getLong(1) : 0L;
            }
        }
    }

    /** Highest Lamport clock persisted in this node's outgoing journal. */
    public synchronized long lastLogicalClock() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery("SELECT COALESCE(MAX(logical_clock), 0) FROM events")) {
            return result.next() ? result.getLong(1) : 0;
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

    public synchronized long size() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery("SELECT COUNT(*) FROM events")) {
            return result.next() ? result.getLong(1) : 0;
        }
    }

    private long lastInsertedOffset() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery("SELECT last_insert_rowid()")) {
            if (!result.next()) {
                throw new SQLException("SQLite did not return the inserted journal offset");
            }
            return result.getLong(1);
        }
    }

    private JournalEntry findConflict(ReplicationEnvelope envelope) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT journal_offset, event_id, world_id, origin_server_id, origin_sequence, world_sequence,
                       logical_clock, protocol_version, created_at, event_type, attributes_json
                FROM events
                WHERE event_id = ? OR (origin_server_id = ? AND origin_sequence = ?)
                LIMIT 1
                """)) {
            query.setString(1, envelope.eventId().toString());
            query.setString(2, envelope.originServerId());
            query.setLong(3, envelope.sequence());
            try (ResultSet result = query.executeQuery()) {
                return result.next() ? readEntry(result) : null;
            }
        }
    }

    private JournalEntry readEntry(ResultSet result) throws SQLException {
        Map<String, String> attributes = gson.fromJson(result.getString("attributes_json"), ATTRIBUTES_TYPE);
        ReplicationEnvelope envelope = new ReplicationEnvelope(
                result.getInt("protocol_version"),
                UUID.fromString(result.getString("event_id")),
                result.getString("world_id"),
                result.getString("origin_server_id"),
                result.getLong("origin_sequence"),
                result.getLong("world_sequence"),
                result.getLong("logical_clock"),
                Instant.parse(result.getString("created_at")),
                result.getString("event_type"),
                attributes
        );
        return new JournalEntry(result.getLong("journal_offset"), envelope);
    }

    @Override
    public synchronized void close() throws SQLException {
        connection.close();
    }
}
