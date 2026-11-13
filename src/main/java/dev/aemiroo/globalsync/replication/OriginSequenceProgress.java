package dev.aemiroo.globalsync.replication;

/**
 * Durable receive-side sequence coverage for one event origin.
 * Sequence numbers are origin-local and start at one.
 */
public record OriginSequenceProgress(
        String originServerId,
        long receivedEvents,
        long highestObservedSequence,
        long firstMissingSequence
) {
    public boolean hasGap() {
        return firstMissingSequence > 0 && firstMissingSequence <= highestObservedSequence;
    }
}
