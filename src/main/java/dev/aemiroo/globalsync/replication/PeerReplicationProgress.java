package dev.aemiroo.globalsync.replication;

import java.time.Instant;

/**
 * A point-in-time view of one peer's outbound replication progress.
 * Local journal offsets are meaningful only on the node that produced this view.
 */
public record PeerReplicationProgress(
        String peerServerId,
        boolean connected,
        long heartbeatPongs,
        long journalHeadOffset,
        long acknowledgedOffset,
        long pendingEvents,
        Instant oldestPendingAt
) {
    public String state() {
        if (!connected) return "DISCONNECTED";
        return pendingEvents > 0 ? "CATCHING_UP" : "CURRENT";
    }
}
