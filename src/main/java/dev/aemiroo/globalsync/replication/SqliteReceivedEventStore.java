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

/** Durable inbound-event inbox, kept separate from the local outgoing journal. */
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
                        world_id TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        UNIQUE (origin_server_id, origin_sequence)
                    )
                    """);
        } catch (SQLException exception) {
            connection.close();
            throw exception;
        }
    }

    /** Returns only after the inbound event is committed with FULL synchronous durability. */
    public synchronized ReceivedEventReceipt accept(ReplicationEnvelope event) throws SQLException {
        String payload = new String(ReplicationMessageCodec.encodeEvent(event), StandardCharsets.UTF_8);
        try {
            connection.setAutoCommit(false);
            try (PreparedStatement query = connection.prepareStatement("""
                    SELECT receipt_offset, payload_json FROM received_events
                    WHERE event_id = ? OR (origin_server_id = ? AND origin_sequence = ?)
                    LIMIT 1
                    """)) {
                query.setString(1, event.eventId().toString());
                query.setString(2, event.originServerId());
                query.setLong(3, event.sequence());
                try (ResultSet result = query.executeQuery()) {
                    if (result.next()) {
                        long offset = result.getLong("receipt_offset");
                        String existing = result.getString("payload_json");
                        if (!existing.equals(payload)) {
                            throw new JournalConflictException(
                                    "Received event ID or origin sequence was reused for different data");
                        }
                        connection.commit();
                        return new ReceivedEventReceipt(offset, true);
                    }
                }
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO received_events
                    (event_id, origin_server_id, origin_sequence, world_id, payload_json)
                    VALUES (?, ?, ?, ?, ?)
                    """)) {
                insert.setString(1, event.eventId().toString());
                insert.setString(2, event.originServerId());
                insert.setLong(3, event.sequence());
                insert.setString(4, event.worldId());
                insert.setString(5, payload);
                insert.executeUpdate();
            }
            long offset;
            try (Statement query = connection.createStatement();
                 ResultSet result = query.executeQuery("SELECT last_insert_rowid()")) {
                if (!result.next()) throw new SQLException("SQLite did not return received-event offset");
                offset = result.getLong(1);
            }
            connection.commit();
            return new ReceivedEventReceipt(offset, false);
        } catch (SQLException | RuntimeException exception) {
            try { connection.rollback(); } catch (SQLException rollbackFailure) {
                exception.addSuppressed(rollbackFailure);
            }
            throw exception;
        } finally {
            try { connection.setAutoCommit(true); } catch (SQLException ignored) { }
        }
    }

    public synchronized long size() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery("SELECT COUNT(*) FROM received_events")) {
            return result.next() ? result.getLong(1) : 0;
        }
    }

    @Override public synchronized void close() throws SQLException { connection.close(); }
}