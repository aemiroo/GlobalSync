package dev.aemiroo.globalsync.network;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSession;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.*;

class PeerCertificateVerifierTest {
    private static SSLSession unusedSession() {
        return (SSLSession) Proxy.newProxyInstance(
                SSLSession.class.getClassLoader(),
                new Class<?>[]{SSLSession.class},
                (proxy, method, args) -> {
                    throw new AssertionError("Invalid fingerprint must be rejected before reading the session");
                });
    }

    @Test
    void rejectsNullSession() {
        assertThrows(NullPointerException.class, () ->
                PeerCertificateVerifier.verifySha256Fingerprint(null, "0".repeat(64)));
    }

    @Test
    void rejectsMalformedFingerprintBeforeInspectingSession() {
        assertThrows(IllegalArgumentException.class, () ->
                PeerCertificateVerifier.verifySha256Fingerprint(unusedSession(), "not-a-fingerprint"));
    }

    @Test
    void rejectsFingerprintWithWrongLength() {
        assertThrows(IllegalArgumentException.class, () ->
                PeerCertificateVerifier.verifySha256Fingerprint(unusedSession(), "a".repeat(63)));
    }
}
