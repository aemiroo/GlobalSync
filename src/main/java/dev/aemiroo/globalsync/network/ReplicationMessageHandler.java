package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.ReplicationEnvelope;
import dev.aemiroo.globalsync.replication.SqliteEventJournal;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/** Persists an allowlisted EVENT before returning its acknowledgement. */
public final class ReplicationMessageHandler {
    private final SqliteEventJournal journal;
    private final Set<String> sharedWorlds;

    public ReplicationMessageHandler(SqliteEventJournal journal, Set<String> sharedWorlds) {
        this.journal = java.util.Objects.requireNonNull(journal, "journal");
        this.sharedWorlds = Set.copyOf(sharedWorlds);
    }

    public byte[] handle(byte[] payload, String authenticatedPeerId) throws IOException {
        ReplicationEnvelope event = ReplicationMessageCodec.decodeEvent(payload, authenticatedPeerId, sharedWorlds);
        try {
            long offset = journal.append(event);
            // SqliteEventJournal uses WAL + synchronous=FULL; append returns only
            // after the insert is committed. Exact retries resolve to the same offset.
            return ReplicationMessageCodec.encodeAck(event.eventId(), offset);
        } catch (SQLException exception) {
            throw new IOException("Could not durably accept replication event", exception);
        }
    }
}