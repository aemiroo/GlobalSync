package dev.aemiroo.globalsync.replication;

/**
 * Durable applied-event coverage for one world and one event origin.
 *
 * @param contiguousAppliedThrough highest sequence for which every sequence
 *                                 from 1 through that value is APPLIED
 * @param highestAppliedSequence highest positive world-local sequence marked APPLIED
 * @param appliedEvents number of positive world-local events marked APPLIED
 */
public record WorldAppliedSequenceProgress(
        String worldId,
        String originServerId,
        long contiguousAppliedThrough,
        long highestAppliedSequence,
        long appliedEvents
) {
    public WorldAppliedSequenceProgress {
        if (worldId == null || worldId.isBlank()) throw new IllegalArgumentException("worldId is required");
        if (originServerId == null || originServerId.isBlank()) throw new IllegalArgumentException("originServerId is required");
        if (contiguousAppliedThrough < 0 || highestAppliedSequence < 0 || appliedEvents < 0)
            throw new IllegalArgumentException("sequence coverage values must not be negative");
        if (contiguousAppliedThrough > highestAppliedSequence)
            throw new IllegalArgumentException("contiguous coverage cannot exceed highest applied sequence");
    }

    public boolean hasGap() {
        return contiguousAppliedThrough < highestAppliedSequence;
    }
}
