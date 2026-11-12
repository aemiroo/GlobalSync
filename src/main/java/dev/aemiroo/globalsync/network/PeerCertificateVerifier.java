package dev.aemiroo.globalsync.network;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Objects;

/**
 * Adds an explicit SHA-256 certificate pin check after normal TLS trust validation.
 */
public final class PeerCertificateVerifier {
    private PeerCertificateVerifier() {
    }

    public static void verifySha256Fingerprint(SSLSession session, String expectedFingerprint)
            throws SSLPeerUnverifiedException {
        Objects.requireNonNull(session, "session");
        if (expectedFingerprint == null || !expectedFingerprint.matches("(?i)[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Expected fingerprint must be 64 hexadecimal characters");
        }

        Certificate[] certificates = session.getPeerCertificates();
        if (certificates.length == 0 || !(certificates[0] instanceof X509Certificate certificate)) {
            throw new SSLPeerUnverifiedException("Peer did not present an X.509 certificate");
        }

        final byte[] actual;
        try {
            actual = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        } catch (java.security.cert.CertificateEncodingException exception) {
            throw new SSLPeerUnverifiedException("Could not encode the peer certificate");
        }

        byte[] expected = HexFormat.of().parseHex(expectedFingerprint.toLowerCase(Locale.ROOT));
        if (!MessageDigest.isEqual(actual, expected)) {
            throw new SSLPeerUnverifiedException("Peer certificate fingerprint is not authorized");
        }
    }
}
