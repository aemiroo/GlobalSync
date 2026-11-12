package dev.aemiroo.globalsync.network;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Owns the bounded TLS peer listener and outbound connections.
 *
 * <p>This milestone authenticates and holds peer connections only. No application
 * messages, replication events, commands, or remote operations are accepted.</p>
 */
public final class PeerConnectionManager implements AutoCloseable {
    private final SSLContext tlsContext;
    private final String localServerId;
    private final PeerAuthorizationRegistry authorization;
    private final int maxFrameBytes;
    private final int handshakeTimeoutMillis;
    private final int maxConnections;
    private final Logger logger;
    private final ThreadPoolExecutor connectionWorkers;
    private final Map<String, SSLSocket> connections = new ConcurrentHashMap<>();
    private final Set<Socket> pendingSockets = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile SSLServerSocket listener;
    private volatile Thread acceptThread;

    public PeerConnectionManager(
            SSLContext tlsContext,
            String localServerId,
            PeerAuthorizationRegistry authorization,
            int maxFrameBytes,
            int maxConnections,
            int handshakeTimeoutMillis,
            Logger logger
    ) {
        this.tlsContext = Objects.requireNonNull(tlsContext, "tlsContext");
        this.localServerId = Objects.requireNonNull(localServerId, "localServerId");
        this.authorization = Objects.requireNonNull(authorization, "authorization");
        this.logger = Objects.requireNonNull(logger, "logger");
        if (!localServerId.matches("[A-Za-z0-9._-]{1,64}")) {
            throw new IllegalArgumentException("localServerId must contain only letters, digits, dot, underscore or hyphen");
        }
        if (maxFrameBytes < 4_096 || maxFrameBytes > FrameCodec.DEFAULT_MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("maxFrameBytes must be between 4096 and 1048576");
        }
        if (maxConnections < 1 || maxConnections > 64) {
            throw new IllegalArgumentException("maxConnections must be between 1 and 64");
        }
        if (handshakeTimeoutMillis < 1_000 || handshakeTimeoutMillis > 30_000) {
            throw new IllegalArgumentException("handshakeTimeoutMillis must be between 1000 and 30000");
        }
        this.maxFrameBytes = maxFrameBytes;
        this.maxConnections = maxConnections;
        this.handshakeTimeoutMillis = handshakeTimeoutMillis;
        this.connectionWorkers = new ThreadPoolExecutor(
                maxConnections,
                maxConnections,
                0L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                task -> {
                    Thread thread = new Thread(task, "GlobalSync-Peer-Worker");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public synchronized void start(InetAddress bindAddress, int port) throws IOException {
        Objects.requireNonNull(bindAddress, "bindAddress");
        if (port < 1 || port > 65_535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("Peer listener is already running");
        }

        SSLServerSocket server = null;
        try {
            server = (SSLServerSocket) tlsContext.getServerSocketFactory().createServerSocket();
            TlsContextFactory.requireMutualTls(server);
            server.setReuseAddress(false);
            server.bind(new InetSocketAddress(bindAddress, port), maxConnections);
            listener = server;
            acceptThread = new Thread(this::acceptLoop, "GlobalSync-Peer-Acceptor");
            acceptThread.setDaemon(true);
            acceptThread.start();
        } catch (IOException | RuntimeException exception) {
            running.set(false);
            closeQuietly(server);
            listener = null;
            throw exception;
        }
    }

    /** Initiates outbound connections without blocking the Paper server thread. */
    public void connectConfiguredPeers() {
        if (!running.get()) {
            throw new IllegalStateException("Peer listener must be started before dialing peers");
        }
        for (PeerEndpoint endpoint : authorization.peers()) {
            try {
                connectionWorkers.execute(() -> connectAndHold(endpoint));
            } catch (java.util.concurrent.RejectedExecutionException exception) {
                logger.warning("Peer connection capacity is full; skipped outbound peer " + endpoint.serverId());
            }
        }
    }

    public boolean isConnected(String peerServerId) {
        SSLSocket socket = connections.get(peerServerId);
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    public int connectedPeerCount() {
        return connections.size();
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket accepted = listener.accept();
                if (!(accepted instanceof SSLSocket sslSocket)) {
                    closeQuietly(accepted);
                    continue;
                }
                pendingSockets.add(sslSocket);
                submitAccepted(sslSocket);
            } catch (IOException exception) {
                if (running.get()) {
                    logger.warning("Peer accept failed: " + exception.getClass().getSimpleName());
                }
            }
        }
    }

    private void submitAccepted(SSLSocket socket) {
        try {
            connectionWorkers.execute(() -> authenticateAndHold(socket));
        } catch (java.util.concurrent.RejectedExecutionException exception) {
            pendingSockets.remove(socket);
            closeQuietly(socket);
            logger.warning("Rejected inbound peer connection because the worker limit was reached.");
        }
    }

    private void connectAndHold(PeerEndpoint endpoint) {
        if (connections.containsKey(endpoint.serverId()) || !running.get()) {
            return;
        }
        Socket plainSocket = new Socket();
        pendingSockets.add(plainSocket);
        SSLSocket tlsSocket = null;
        try {
            plainSocket.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), handshakeTimeoutMillis);
            tlsSocket = (SSLSocket) tlsContext.getSocketFactory()
                    .createSocket(plainSocket, endpoint.host(), endpoint.port(), true);
            pendingSockets.remove(plainSocket);
            pendingSockets.add(tlsSocket);
            TlsContextFactory.configureClientSocket(tlsSocket);
            tlsSocket.setSoTimeout(handshakeTimeoutMillis);
            authenticateAndHold(tlsSocket);
        } catch (IOException | RuntimeException exception) {
            closeQuietly(tlsSocket);
            logger.warning("Could not authenticate peer " + endpoint.serverId() + ": "
                    + exception.getClass().getSimpleName());
        } finally {
            pendingSockets.remove(plainSocket);
            closeQuietly(plainSocket);
        }
    }

    private void authenticateAndHold(SSLSocket socket) {
        String peerId = null;
        try {
            socket.setSoTimeout(handshakeTimeoutMillis);
            peerId = PeerHandshake.authenticate(socket, localServerId, authorization, maxFrameBytes);
            SSLSocket previous = connections.putIfAbsent(peerId, socket);
            if (previous != null) {
                throw new IOException("A connection for this peer is already active");
            }
            socket.setSoTimeout(0);
            logger.info("Authenticated peer connection established: " + peerId);

            // Application frames are deliberately not implemented in this milestone.
            // Keep the authenticated channel open and detect peer disconnects. Any
            // post-handshake data is rejected until a versioned message allowlist exists.
            int unexpectedData = socket.getInputStream().read();
            if (unexpectedData != -1) {
                throw new IOException("Application messages are not enabled in this protocol version");
            }
        } catch (IOException | RuntimeException exception) {
            if (peerId != null) {
                logger.info("Peer connection closed: " + peerId);
            } else {
                logger.warning("Rejected unauthenticated peer connection: "
                        + exception.getClass().getSimpleName());
            }
        } finally {
            if (peerId != null) {
                connections.remove(peerId, socket);
            }
            pendingSockets.remove(socket);
            closeQuietly(socket);
        }
    }

    @Override
    public synchronized void close() {
        if (!running.getAndSet(false)) {
            connectionWorkers.shutdownNow();
            return;
        }
        closeQuietly(listener);
        listener = null;
        for (Socket socket : pendingSockets) {
            closeQuietly(socket);
        }
        pendingSockets.clear();
        connections.clear();
        connectionWorkers.shutdownNow();
        try {
            connectionWorkers.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        Thread thread = acceptThread;
        if (thread != null) {
            thread.interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignored) {
            // Closing during shutdown is best-effort.
        }
    }
}
