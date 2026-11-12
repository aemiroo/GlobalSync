package dev.aemiroo.globalsync.network;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;

class PeerConnectionManagerIntegrationTest {
    private static final String STORE_PASSWORD = "test-only-password";
    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static Path identityA;
    private static Path trustA;
    private static Path identityB;
    private static Path trustB;
    private static String fingerprintA;
    private static String fingerprintB;
    private static SSLContext contextA;
    private static SSLContext contextB;

    @BeforeAll
    static void createMutualTlsFixtures(@TempDir Path directory) throws Exception {
        identityA = directory.resolve("node-a.p12");
        trustA = directory.resolve("trust-a.p12");
        identityB = directory.resolve("node-b.p12");
        trustB = directory.resolve("trust-b.p12");
        Path certificateA = directory.resolve("node-a.cer");
        Path certificateB = directory.resolve("node-b.cer");

        runKeytool("-genkeypair", "-alias", "node", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "30", "-dname", "CN=GlobalSync Test A", "-ext", "SAN=ip:127.0.0.1",
                "-storetype", "PKCS12", "-keystore", identityA.toString(),
                "-storepass", STORE_PASSWORD, "-keypass", STORE_PASSWORD, "-noprompt");
        runKeytool("-genkeypair", "-alias", "node", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "30", "-dname", "CN=GlobalSync Test B", "-ext", "SAN=ip:127.0.0.1",
                "-storetype", "PKCS12", "-keystore", identityB.toString(),
                "-storepass", STORE_PASSWORD, "-keypass", STORE_PASSWORD, "-noprompt");
        runKeytool("-exportcert", "-alias", "node", "-keystore", identityA.toString(),
                "-storepass", STORE_PASSWORD, "-file", certificateA.toString());
        runKeytool("-exportcert", "-alias", "node", "-keystore", identityB.toString(),
                "-storepass", STORE_PASSWORD, "-file", certificateB.toString());
        runKeytool("-importcert", "-alias", "peer-b", "-file", certificateB.toString(),
                "-keystore", trustA.toString(), "-storetype", "PKCS12",
                "-storepass", STORE_PASSWORD, "-noprompt");
        runKeytool("-importcert", "-alias", "peer-a", "-file", certificateA.toString(),
                "-keystore", trustB.toString(), "-storetype", "PKCS12",
                "-storepass", STORE_PASSWORD, "-noprompt");

        fingerprintA = fingerprint(identityA);
        fingerprintB = fingerprint(identityB);
        char[] passwordA = STORE_PASSWORD.toCharArray();
        char[] trustPasswordA = STORE_PASSWORD.toCharArray();
        char[] passwordB = STORE_PASSWORD.toCharArray();
        char[] trustPasswordB = STORE_PASSWORD.toCharArray();
        try {
            contextA = TlsContextFactory.create(identityA, passwordA, trustA, trustPasswordA);
            contextB = TlsContextFactory.create(identityB, passwordB, trustB, trustPasswordB);
        } finally {
            java.util.Arrays.fill(passwordA, '\0');
            java.util.Arrays.fill(trustPasswordA, '\0');
            java.util.Arrays.fill(passwordB, '\0');
            java.util.Arrays.fill(trustPasswordB, '\0');
        }
    }

    @Test
    void establishesAuthenticatedMutualTlsPeerChannel() throws Exception {
        int portA = availablePort();
        int portB;
        do {
            portB = availablePort();
        } while (portA == portB);
        PeerConnectionManager managerA = manager(contextA, "a-region",
                new PeerEndpoint("b-region", "127.0.0.1", portB, fingerprintB));
        PeerConnectionManager managerB = manager(contextB, "b-region",
                new PeerEndpoint("a-region", "127.0.0.1", portA, fingerprintA));
        try (managerA; managerB) {
            managerB.start(LOOPBACK, portB);
            managerA.start(LOOPBACK, portA);
            managerA.connectConfiguredPeers();

            assertTrue(await(() -> managerA.connectedPeerCount() == 1
                    && managerB.connectedPeerCount() == 1, 5, TimeUnit.SECONDS),
                    "both nodes should register the same authenticated TLS channel");
        }
    }

    @Test
    void rejectsPeerWithIncorrectPinnedFingerprint() throws Exception {
        int portA = availablePort();
        int portB = availablePort();
        PeerConnectionManager managerA = manager(contextA, "a-region",
                new PeerEndpoint("b-region", "127.0.0.1", portB, "0".repeat(64)));
        PeerConnectionManager managerB = manager(contextB, "b-region",
                new PeerEndpoint("a-region", "127.0.0.1", portA, fingerprintA));
        try (managerA; managerB) {
            managerB.start(LOOPBACK, portB);
            managerA.start(LOOPBACK, portA);
            managerA.connectConfiguredPeers();

            Thread.sleep(500);
            assertEquals(0, managerA.connectedPeerCount());
            assertEquals(0, managerB.connectedPeerCount());
        }
    }

    private static PeerConnectionManager manager(
            SSLContext context,
            String localId,
            PeerEndpoint remote
    ) {
        return new PeerConnectionManager(
                context,
                localId,
                new PeerAuthorizationRegistry(List.of(remote)),
                FrameCodec.DEFAULT_MAX_FRAME_BYTES,
                4,
                3_000,
                Logger.getLogger("GlobalSync-PeerConnectionManager-Test")
        );
    }

    private static boolean await(java.util.function.BooleanSupplier condition, long timeout, TimeUnit unit)
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return condition.getAsBoolean();
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0, 1, LOOPBACK)) {
            return socket.getLocalPort();
        }
    }

    private static String fingerprint(Path identityStore) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var input = java.nio.file.Files.newInputStream(identityStore)) {
            store.load(input, STORE_PASSWORD.toCharArray());
        }
        Certificate certificate = store.getCertificate("node");
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
        return HexFormat.of().formatHex(digest);
    }

    private static void runKeytool(String... arguments) throws Exception {
        String executable = System.getProperty("os.name").toLowerCase().contains("win") ? "keytool.exe" : "keytool";
        Path keytool = Path.of(System.getProperty("java.home"), "bin", executable);
        Process process = new ProcessBuilder()
                .command(buildCommand(keytool.toString(), arguments))
                .redirectErrorStream(true)
                .start();
        String output;
        try (var input = process.getInputStream()) {
            output = new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
        if (!process.waitFor(15, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IOException("keytool timed out");
        }
        if (process.exitValue() != 0) {
            throw new IOException("keytool failed: " + output);
        }
    }

    private static List<String> buildCommand(String executable, String[] arguments) {
        var command = new java.util.ArrayList<String>();
        command.add(executable);
        command.addAll(List.of(arguments));
        return command;
    }
}
