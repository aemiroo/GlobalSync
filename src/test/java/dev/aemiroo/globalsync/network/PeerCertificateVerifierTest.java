package dev.aemiroo.globalsync.network;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PeerCertificateVerifierTest {
    @Test
    void rejectsNullSession() {
        assertThrows(NullPointerException.class, () ->
                PeerCertificateVerifier.verifySha256Fingerprint(null, "0".repeat(64)));
    }

    @Test
    void rejectsMalformedFingerprintBeforeInspectingSession() {
        assertThrows(IllegalArgumentException.class, () ->
                PeerCertificateVerifier.verifySha256Fingerprint(null, "not-a-fingerprint"));
    }

    @Test
    void rejectsFingerprintWithWrongLength() {
        assertThrows(IllegalArgumentException.class, () ->
                PeerCertificateVerifier.verifySha256Fingerprint(null, "a".repeat(63)));
    }
}
