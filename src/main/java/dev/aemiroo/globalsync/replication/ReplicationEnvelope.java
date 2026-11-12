package dev.aemiroo.globalsync.replication;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Versioned event metadata shared by future replication transports.
 * Payloads are deliberately opaque here; encoding and durable delivery are separate layers.
 */
public record ReplicationEnvelope(
        int protocolVersion,
        UUID eventId,
        String worldId,
        String originServerId,
        long sequence,
        Instant createdAt,
        String eventType,
        Map<String, String> attributes
) {
    public static final int CURRENT_PROTOCOL_VERSION = 1;

    public ReplicationEnvelope {
        if (protocolVersion != CURRENT_PROTOCOL_VERSION) {
            throw new IllegalArgumentException("Unsupported protocol version: " + protocolVersion);
        }
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(attributes, "attributes");
        requireText(worldId, "worldId");
        requireText(originServerId, "originServerId");
        requireText(eventType, "eventType");
        if (sequence < 1) {
            throw new IllegalArgumentException("sequence must be positive");
        }
        attributes = Map.copyOf(attributes);
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
