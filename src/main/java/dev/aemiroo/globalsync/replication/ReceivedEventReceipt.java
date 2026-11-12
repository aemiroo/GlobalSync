package dev.aemiroo.globalsync.replication;

/** Durable inbox offset returned after the receiving transaction commits. */
public record ReceivedEventReceipt(long offset, boolean duplicate) {
    public ReceivedEventReceipt {
        if (offset < 1) throw new IllegalArgumentException("offset must be positive");
    }
}