package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.ReplicationEnvelope;
import dev.aemiroo.globalsync.replication.JournalConflictException;
import dev.aemiroo.globalsync.replication.SqliteReceivedEventStore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;

/** Persists an allowlisted EVENT before returning its acknowledgement. */
public final class ReplicationMessageHandler {
    private final SqliteReceivedEventStore receivedEvents;
    private final Set<String> sharedWorlds;

    public ReplicationMessageHandler(SqliteReceivedEventStore receivedEvents, Set<String> sharedWorlds) {
        this.receivedEvents = java.util.Objects.requireNonNull(receivedEvents, "receivedEvents");
        this.sharedWorlds = Set.copyOf(sharedWorlds);
    }

    public byte[] handle(byte[] payload, String authenticatedPeerId) throws IOException {
        ReplicationEnvelope event = ReplicationMessageCodec.decodeEvent(payload, authenticatedPeerId, sharedWorlds);
        try {
            var receipt = receivedEvents.accept(event);
            // ACK is sent only after the durable inbox transaction has committed.
            return ReplicationMessageCodec.encodeAck(event.eventId(), receipt.offset());
        } catch (SQLException | JournalConflictException exception) {
            throw new IOException("Could not durably accept replication event", exception);
        }
    }
}