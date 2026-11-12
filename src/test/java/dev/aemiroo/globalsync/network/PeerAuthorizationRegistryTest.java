package dev.aemiroo.globalsync.network;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.lang.reflect.Proxy;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PeerAuthorizationRegistryTest {
    private static final String FINGERPRINT = "b".repeat(64);

    private static SSLSession unusedSession() {
        return (SSLSession) Proxy.newProxyInstance(
                SSLSession.class.getClassLoader(),
                new Class<?>[]{SSLSession.class},
                (proxy, method, args) -> {
                    throw new AssertionError("Unknown peer IDs must be rejected before inspecting the TLS session");
                });
    }

    @Test
    void rejectsUnknownPeerBeforeReadingTlsSession() {
        PeerAuthorizationRegistry registry = new PeerAuthorizationRegistry(List.of(
                new PeerEndpoint("region-europe", "eu.example.net", 28120, FINGERPRINT)
        ));

        assertThrows(SSLPeerUnverifiedException.class,
                () -> registry.requireAuthorized(unusedSession(), "region-unknown"));
    }

    @Test
    void rejectsDuplicatePeerIds() {
        PeerEndpoint endpoint = new PeerEndpoint("region-europe", "eu.example.net", 28120, FINGERPRINT);

        assertThrows(IllegalArgumentException.class,
                () -> new PeerAuthorizationRegistry(List.of(endpoint, endpoint)));
    }

    @Test
    void exposesOnlyConfiguredPeers() {
        PeerEndpoint endpoint = new PeerEndpoint("region-europe", "eu.example.net", 28120, FINGERPRINT);
        PeerAuthorizationRegistry registry = new PeerAuthorizationRegistry(List.of(endpoint));

        assertEquals(1, registry.size());
        assertSame(endpoint, registry.find("region-europe"));
        assertNull(registry.find("region-unknown"));
    }
}
