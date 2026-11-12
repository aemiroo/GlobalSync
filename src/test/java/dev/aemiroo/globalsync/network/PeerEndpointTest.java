package dev.aemiroo.globalsync.network;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PeerEndpointTest {
    private static final String FINGERPRINT = "a".repeat(64);

    @Test
    void acceptsValidPeerEndpointAndNormalizesFingerprint() {
        PeerEndpoint endpoint = new PeerEndpoint("region-europe", "eu.example.net", 28120, FINGERPRINT.toUpperCase());

        assertEquals("region-europe", endpoint.serverId());
        assertEquals("eu.example.net", endpoint.host());
        assertEquals(28120, endpoint.port());
        assertEquals(FINGERPRINT, endpoint.sha256Fingerprint());
    }

    @Test
    void rejectsInvalidServerId() {
        assertThrows(IllegalArgumentException.class,
                () -> new PeerEndpoint("region europe", "eu.example.net", 28120, FINGERPRINT));
    }

    @Test
    void rejectsInvalidHost() {
        assertThrows(IllegalArgumentException.class,
                () -> new PeerEndpoint("region-europe", "https://eu.example.net", 28120, FINGERPRINT));
    }

    @Test
    void rejectsOutOfRangePort() {
        assertThrows(IllegalArgumentException.class,
                () -> new PeerEndpoint("region-europe", "eu.example.net", 65_536, FINGERPRINT));
    }

    @Test
    void rejectsMalformedFingerprint() {
        assertThrows(IllegalArgumentException.class,
                () -> new PeerEndpoint("region-europe", "eu.example.net", 28120, "abc"));
    }
}
