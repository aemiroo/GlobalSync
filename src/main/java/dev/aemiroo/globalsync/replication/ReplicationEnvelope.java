package dev.aemiroo.globalsync.replication;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Versioned event metadata with deterministic cross-node conflict ordering. */
public record ReplicationEnvelope(
        int protocolVersion, UUID eventId, String worldId, String originServerId,
        long sequence, long logicalClock, Instant createdAt, String eventType,
        Map<String, String> attributes
) {
    public static final int CURRENT_PROTOCOL_VERSION = 2;

    public ReplicationEnvelope {
        if (protocolVersion != CURRENT_PROTOCOL_VERSION)
            throw new IllegalArgumentException("Unsupported protocol version: " + protocolVersion);
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(attributes, "attributes");
        requireText(worldId, "worldId");
        requireText(originServerId, "originServerId");
        requireText(eventType, "eventType");
        if (sequence < 1) throw new IllegalArgumentException("sequence must be positive");
        if (logicalClock < 1 || logicalClock > LogicalClock.MAX_VALUE)
            throw new IllegalArgumentException("logicalClock is outside the supported range");
        attributes = Map.copyOf(attributes);
    }

    /** Compatibility constructor; production capture supplies a Lamport clock explicitly. */
    public ReplicationEnvelope(int protocolVersion, UUID eventId, String worldId,
            String originServerId, long sequence, Instant createdAt, String eventType,
            Map<String, String> attributes) {
        this(protocolVersion, eventId, worldId, originServerId, sequence, sequence,
                createdAt, eventType, attributes);
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) throw new IllegalArgumentException(name + " must not be blank");
    }
}
