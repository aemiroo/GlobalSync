package dev.aemiroo.globalsync.network;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.KeyManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Objects;

/**
 * Loads a TLS 1.3 context from operator-provisioned PKCS12 identity and trust stores.
 * The caller owns and must clear the supplied password arrays.
 */
public final class TlsContextFactory {
    private TlsContextFactory() {
    }

    public static SSLContext create(
            Path identityStorePath,
            char[] identityStorePassword,
            Path trustStorePath,
            char[] trustStorePassword
    ) throws GeneralSecurityException, IOException {
        Objects.requireNonNull(identityStorePath, "identityStorePath");
        Objects.requireNonNull(identityStorePassword, "identityStorePassword");
        Objects.requireNonNull(trustStorePath, "trustStorePath");
        Objects.requireNonNull(trustStorePassword, "trustStorePassword");

        KeyStore identityStore = loadStore(identityStorePath, identityStorePassword);
        KeyStore trustStore = loadStore(trustStorePath, trustStorePassword);

        KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(identityStore, identityStorePassword);

        TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(trustStore);

        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
        return context;
    }

    public static void requireMutualTls(SSLServerSocket socket) {
        Objects.requireNonNull(socket, "socket");
        socket.setEnabledProtocols(new String[]{"TLSv1.3"});
        socket.setNeedClientAuth(true);
    }

    public static void configureClientSocket(SSLSocket socket) {
        Objects.requireNonNull(socket, "socket");
        socket.setEnabledProtocols(new String[]{"TLSv1.3"});
    }

    private static KeyStore loadStore(Path path, char[] password)
            throws GeneralSecurityException, IOException {
        if (!Files.isRegularFile(path)) {
            throw new IOException("TLS store does not exist or is not a regular file: " + path);
        }
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(path)) {
            store.load(input, password);
        }
        return store;
    }
}
