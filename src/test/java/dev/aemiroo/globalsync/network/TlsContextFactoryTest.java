package dev.aemiroo.globalsync.network;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class TlsContextFactoryTest {
    @Test
    void clientRequiresTls13AndHostnameVerification() throws Exception {
        try (SSLSocket socket = (SSLSocket) SSLContext.getDefault().getSocketFactory().createSocket()) {
            TlsContextFactory.configureClientSocket(socket);

            assertArrayEquals(new String[]{"TLSv1.3"}, socket.getEnabledProtocols());
            assertEquals("HTTPS", socket.getSSLParameters().getEndpointIdentificationAlgorithm());
        }
    }

    @Test
    void serverRequiresTls13AndClientCertificates() throws Exception {
        try (SSLServerSocket socket = (SSLServerSocket) SSLContext.getDefault()
                .getServerSocketFactory().createServerSocket()) {
            TlsContextFactory.requireMutualTls(socket);

            assertArrayEquals(new String[]{"TLSv1.3"}, socket.getEnabledProtocols());
            assertTrue(socket.getNeedClientAuth());
        }
    }

    @Test
    void rejectsNullClientSocket() {
        assertThrows(NullPointerException.class, () -> TlsContextFactory.configureClientSocket(null));
    }

    @Test
    void rejectsNullServerSocket() {
        assertThrows(NullPointerException.class, () -> TlsContextFactory.requireMutualTls(null));
    }
}
