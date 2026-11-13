package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.JournalConflictException;
import dev.aemiroo.globalsync.replication.LogicalClock;
import dev.aemiroo.globalsync.replication.ReplicationEnvelope;
import dev.aemiroo.globalsync.replication.SqliteReceivedEventStore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import java.util.function.Consumer;

/** Validates and durably accepts an EVENT before returning its acknowledgement. */
public final class ReplicationMessageHandler {
    private final SqliteReceivedEventStore receivedEvents;
    private final Set<String> sharedWorlds;
    private final LogicalClock logicalClock;
    private final Consumer<ReplicationEnvelope> applyRequest;

    public ReplicationMessageHandler(SqliteReceivedEventStore receivedEvents, Set<String> sharedWorlds) {
        this(receivedEvents, sharedWorlds, null, ignored -> { });
    }

    public ReplicationMessageHandler(SqliteReceivedEventStore receivedEvents, Set<String> sharedWorlds,
            LogicalClock logicalClock, Consumer<ReplicationEnvelope> applyRequest) {
        this.receivedEvents = java.util.Objects.requireNonNull(receivedEvents, "receivedEvents");
        this.sharedWorlds = Set.copyOf(sharedWorlds);
        this.logicalClock = logicalClock;
        this.applyRequest = java.util.Objects.requireNonNull(applyRequest, "applyRequest");
    }

    public byte[] handle(byte[] payload, String authenticatedPeerId) throws IOException {
        ReplicationEnvelope event = ReplicationMessageCodec.decodeEvent(payload, authenticatedPeerId, sharedWorlds);
        try {
            // Advance before accepting so a local edit racing with this receive
            // cannot be assigned a clock that precedes an already observed event.
            if (logicalClock != null) logicalClock.observe(event.logicalClock());
            var receipt = receivedEvents.accept(event);
            if (receipt.applyRequired()) applyRequest.accept(event);
            // ACK is emitted only after the inbox and per-block winner commit.
            return ReplicationMessageCodec.encodeAck(event.eventId(), receipt.offset());
        } catch (SQLException | JournalConflictException | IllegalStateException exception) {
            throw new IOException("Could not durably accept replication event", exception);
        }
    }
}
