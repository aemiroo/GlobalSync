package dev.aemiroo.globalsync.network;

import java.util.UUID;

/** ACK returned by a receiver after committing an event to its durable inbox. */
public record ReplicationAcknowledgement(UUID eventId, long receiverReceiptOffset) {
    public ReplicationAcknowledgement {
        if (eventId == null) throw new NullPointerException("eventId");
        if (receiverReceiptOffset < 1) throw new IllegalArgumentException("receiverReceiptOffset must be positive");
    }
}