package dev.aemiroo.globalsync.network;

import java.util.Objects;

/** Operator-configured endpoint and pinned identity for one remote region. */
public record PeerEndpoint(String serverId, String host, int port, String sha256Fingerprint) {
    public PeerEndpoint {
        requireText(serverId, "serverId");
        requireText(host, "host");
        Objects.requireNonNull(sha256Fingerprint, "sha256Fingerprint");
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (!sha256Fingerprint.matches("(?i)[0-9a-f]{64}")) {
            throw new IllegalArgumentException("SHA-256 fingerprint must be 64 hexadecimal characters");
        }
        if (serverId.length() > 64 || host.length() > 253) {
            throw new IllegalArgumentException("Peer identity or host is too long");
        }
        sha256Fingerprint = sha256Fingerprint.toLowerCase(java.util.Locale.ROOT);
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.length() > 253) {
            throw new IllegalArgumentException(name + " must be non-blank and at most 253 characters");
        }
    }
}
