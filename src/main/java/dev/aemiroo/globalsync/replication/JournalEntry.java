package dev.aemiroo.globalsync.replication;

import java.util.Objects;

/**
 * A durable journal record and its monotonically increasing local offset.
 */
public record JournalEntry(long offset, ReplicationEnvelope envelope) {
    public JournalEntry {
        if (offset < 1) {
            throw new IllegalArgumentException("offset must be positive");
        }
        Objects.requireNonNull(envelope, "envelope");
    }
}
