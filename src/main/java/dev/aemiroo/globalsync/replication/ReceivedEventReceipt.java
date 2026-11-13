package dev.aemiroo.globalsync.replication;

/** Durable inbox receipt and whether this event is the current block winner awaiting application. */
public record ReceivedEventReceipt(long offset, boolean duplicate, boolean applyRequired) {
    public ReceivedEventReceipt {
        if (offset < 1) throw new IllegalArgumentException("offset must be positive");
    }

    public ReceivedEventReceipt(long offset, boolean duplicate) {
        this(offset, duplicate, false);
    }
}
