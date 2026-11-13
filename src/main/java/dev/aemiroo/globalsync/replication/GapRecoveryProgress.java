package dev.aemiroo.globalsync.replication;

import java.time.Instant;

/** Durable operator-facing state for one exact, unresolved origin sequence gap. */
public record GapRecoveryProgress(
        String originServerId,
        long originSequence,
        String status,
        Instant firstDetectedAt,
        Instant lastCheckedAt,
        Instant unavailableAt
) {
    public boolean unavailable() {
        return "UNAVAILABLE".equals(status);
    }
}
