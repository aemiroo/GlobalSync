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
    private final PeerMessageRateLimiter rateLimiter;

    public ReplicationMessageHandler(SqliteReceivedEventStore receivedEvents, Set<String> sharedWorlds) {
        this(receivedEvents, sharedWorlds, null, ignored -> { }, PeerMessageRateLimiter.defaults());
    }

    public ReplicationMessageHandler(SqliteReceivedEventStore receivedEvents, Set<String> sharedWorlds,
            LogicalClock logicalClock, Consumer<ReplicationEnvelope> applyRequest) {
        this(receivedEvents, sharedWorlds, logicalClock, applyRequest, PeerMessageRateLimiter.defaults());
    }

    public ReplicationMessageHandler(SqliteReceivedEventStore receivedEvents, Set<String> sharedWorlds,
            LogicalClock logicalClock, Consumer<ReplicationEnvelope> applyRequest,
            PeerMessageRateLimiter rateLimiter) {
        this.receivedEvents = java.util.Objects.requireNonNull(receivedEvents, "receivedEvents");
        this.sharedWorlds = Set.copyOf(sharedWorlds);
        this.logicalClock = logicalClock;
        this.applyRequest = java.util.Objects.requireNonNull(applyRequest, "applyRequest");
        this.rateLimiter = java.util.Objects.requireNonNull(rateLimiter, "rateLimiter");
    }

    public Set<String> sharedWorlds() { return sharedWorlds; }

    /** Returns the first durable sequence hole for an authenticated event origin. */
    public long firstMissingSequenceForOrigin(String originServerId) throws IOException {
        if (originServerId == null || !originServerId.matches("[A-Za-z0-9._-]{1,64}"))
            throw new IOException("Invalid event origin");
        try {
            return receivedEvents.originSequenceProgress().stream()
                    .filter(progress -> progress.originServerId().equals(originServerId))
                    .filter(dev.aemiroo.globalsync.replication.OriginSequenceProgress::hasGap)
                    .mapToLong(dev.aemiroo.globalsync.replication.OriginSequenceProgress::firstMissingSequence)
                    .findFirst().orElse(0L);
        } catch (SQLException exception) {
            throw new IOException("Could not inspect durable origin sequence coverage", exception);
        }
    }

    public byte[] handle(byte[] payload, String authenticatedPeerId) throws IOException {
        if (payload == null || authenticatedPeerId == null || authenticatedPeerId.isBlank())
            throw new IOException("Missing replication payload or authenticated peer identity");
        if (!rateLimiter.tryAcquire(authenticatedPeerId, payload.length))
            throw new IOException("Authenticated peer exceeded the inbound replication rate limit");

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
