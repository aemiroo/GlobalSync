package dev.aemiroo.globalsync.replication;

/**
 * Raised when an event ID or per-node sequence is reused for different event data.
 */
public final class JournalConflictException extends RuntimeException {
    public JournalConflictException(String message) {
        super(message);
    }
}
