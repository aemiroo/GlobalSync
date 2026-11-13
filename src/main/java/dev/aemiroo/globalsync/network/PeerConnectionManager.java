package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.JournalEntry;
import dev.aemiroo.globalsync.replication.SqliteEventJournal;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Owns authenticated full-duplex peer sessions. One reader handles inbound
 * EVENT/ACK frames; a bounded sender performs stop-and-wait journal delivery.
 */
public final class PeerConnectionManager implements AutoCloseable {
    private final SSLContext tlsContext;
    private final String localServerId;
    private final PeerAuthorizationRegistry authorization;
    private final int maxFrameBytes;
    private final int handshakeTimeoutMillis;
    private final int maxConnections;
    private final int acknowledgementTimeoutMillis;
    private final int deliveryPollMillis;
    private final int reconnectMinMillis;
    private final int reconnectMaxMillis;
    private final int heartbeatIntervalMillis;
    private final int heartbeatTimeoutMillis;
    private final Logger logger;
    private final ReplicationMessageHandler messageHandler;
    private final SqliteEventJournal eventJournal;
    private final ThreadPoolExecutor connectionWorkers;
    private final ThreadPoolExecutor deliveryWorkers;
    private final ScheduledExecutorService heartbeatWorker;
    private final PeerMessageRateLimiter controlRateLimiter =
            new PeerMessageRateLimiter(64, 10.0, 20.0, 16_384.0, 32_768.0);
    private final Map<String, PeerSession> connections = new ConcurrentHashMap<>();
    private final Set<Socket> pendingSockets = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile SSLServerSocket listener;
    private volatile Thread acceptThread;

    public PeerConnectionManager(SSLContext tlsContext, String localServerId,
            PeerAuthorizationRegistry authorization, int maxFrameBytes, int maxConnections,
            int handshakeTimeoutMillis, Logger logger) {
        this(tlsContext, localServerId, authorization, maxFrameBytes, maxConnections,
                handshakeTimeoutMillis, logger, null, null, 10_000, 250, 1_000, 30_000);
    }

    public PeerConnectionManager(SSLContext tlsContext, String localServerId,
            PeerAuthorizationRegistry authorization, int maxFrameBytes, int maxConnections,
            int handshakeTimeoutMillis, Logger logger, ReplicationMessageHandler messageHandler) {
        this(tlsContext, localServerId, authorization, maxFrameBytes, maxConnections,
                handshakeTimeoutMillis, logger, messageHandler, null, 10_000, 250, 1_000, 30_000);
    }

    public PeerConnectionManager(SSLContext tlsContext, String localServerId,
            PeerAuthorizationRegistry authorization, int maxFrameBytes, int maxConnections,
            int handshakeTimeoutMillis, Logger logger, ReplicationMessageHandler messageHandler,
            SqliteEventJournal eventJournal, int acknowledgementTimeoutMillis,
            int deliveryPollMillis, int reconnectMinMillis, int reconnectMaxMillis) {
        this(tlsContext, localServerId, authorization, maxFrameBytes, maxConnections,
                handshakeTimeoutMillis, logger, messageHandler, eventJournal,
                acknowledgementTimeoutMillis, deliveryPollMillis, reconnectMinMillis,
                reconnectMaxMillis, 15_000, 45_000);
    }

    public PeerConnectionManager(SSLContext tlsContext, String localServerId,
            PeerAuthorizationRegistry authorization, int maxFrameBytes, int maxConnections,
            int handshakeTimeoutMillis, Logger logger, ReplicationMessageHandler messageHandler,
            SqliteEventJournal eventJournal, int acknowledgementTimeoutMillis,
            int deliveryPollMillis, int reconnectMinMillis, int reconnectMaxMillis,
            int heartbeatIntervalMillis, int heartbeatTimeoutMillis) {
        this.tlsContext = Objects.requireNonNull(tlsContext, "tlsContext");
        this.localServerId = Objects.requireNonNull(localServerId, "localServerId");
        this.authorization = Objects.requireNonNull(authorization, "authorization");
        this.logger = Objects.requireNonNull(logger, "logger");
        this.messageHandler = messageHandler;
        this.eventJournal = eventJournal;
        if (!localServerId.matches("[A-Za-z0-9._-]{1,64}"))
            throw new IllegalArgumentException("Invalid local server ID");
        if (maxFrameBytes < 4_096 || maxFrameBytes > FrameCodec.DEFAULT_MAX_FRAME_BYTES)
            throw new IllegalArgumentException("maxFrameBytes must be between 4096 and 1048576");
        if (maxConnections < 1 || maxConnections > 64)
            throw new IllegalArgumentException("maxConnections must be between 1 and 64");
        if (authorization.peers().size() > maxConnections)
            throw new IllegalArgumentException("Configured peer count cannot exceed maxConnections");
        if (handshakeTimeoutMillis < 1_000 || handshakeTimeoutMillis > 30_000)
            throw new IllegalArgumentException("handshakeTimeoutMillis must be between 1000 and 30000");
        if (acknowledgementTimeoutMillis < 1_000 || acknowledgementTimeoutMillis > 120_000)
            throw new IllegalArgumentException("acknowledgementTimeoutMillis must be between 1000 and 120000");
        if (deliveryPollMillis < 50 || deliveryPollMillis > 10_000)
            throw new IllegalArgumentException("deliveryPollMillis must be between 50 and 10000");
        if (reconnectMinMillis < 250 || reconnectMaxMillis < reconnectMinMillis || reconnectMaxMillis > 300_000)
            throw new IllegalArgumentException("Invalid reconnect backoff bounds");
        if (heartbeatIntervalMillis < 1_000 || heartbeatIntervalMillis > 120_000)
            throw new IllegalArgumentException("heartbeatIntervalMillis must be between 1000 and 120000");
        if (heartbeatTimeoutMillis < heartbeatIntervalMillis
                || heartbeatTimeoutMillis > 300_000)
            throw new IllegalArgumentException("heartbeatTimeoutMillis must be at least the interval and at most 300000");
        if ((messageHandler == null) != (eventJournal == null))
            throw new IllegalArgumentException("Inbound handler and outgoing journal must be configured together");
        this.maxFrameBytes = maxFrameBytes;
        this.maxConnections = maxConnections;
        this.handshakeTimeoutMillis = handshakeTimeoutMillis;
        this.acknowledgementTimeoutMillis = acknowledgementTimeoutMillis;
        this.deliveryPollMillis = deliveryPollMillis;
        this.reconnectMinMillis = reconnectMinMillis;
        this.reconnectMaxMillis = reconnectMaxMillis;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.connectionWorkers = workerPool(maxConnections, "GlobalSync-Peer-Worker");
        this.deliveryWorkers = workerPool(maxConnections, "GlobalSync-Delivery-Worker");
        this.heartbeatWorker = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "GlobalSync-Heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        this.heartbeatWorker.scheduleAtFixedRate(this::checkHeartbeats, 1, 1, TimeUnit.SECONDS);
    }

    private static ThreadPoolExecutor workerPool(int size, String name) {
        return new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(), task -> {
                    Thread thread = new Thread(task, name);
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    public synchronized void start(InetAddress bindAddress, int port) throws IOException {
        Objects.requireNonNull(bindAddress, "bindAddress");
        if (port < 1 || port > 65_535) throw new IllegalArgumentException("Invalid port");
        if (!running.compareAndSet(false, true)) throw new IllegalStateException("Peer listener is already running");
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

    /** Starts one bounded reconnect loop for each deterministically selected dialer peer. */
    public void connectConfiguredPeers() {
        if (!running.get()) throw new IllegalStateException("Peer listener must be started before dialing peers");
        for (PeerEndpoint endpoint : authorization.peers()) {
            if (localServerId.compareTo(endpoint.serverId()) > 0) continue;
            try {
                connectionWorkers.execute(() -> connectWithRetry(endpoint));
            } catch (java.util.concurrent.RejectedExecutionException exception) {
                logger.warning("Peer connection capacity is full; could not schedule " + endpoint.serverId());
            }
        }
    }

    public boolean isConnected(String peerServerId) {
        PeerSession session = connections.get(peerServerId);
        return session != null && session.isOpen();
    }
    public int connectedPeerCount() { return connections.size(); }

    private void acceptLoop() {
        SSLServerSocket server = listener;
        while (running.get() && server != null) {
            try {
                Socket accepted = server.accept();
                if (!(accepted instanceof SSLSocket sslSocket)) { closeQuietly(accepted); continue; }
                pendingSockets.add(sslSocket);
                submitAccepted(sslSocket);
            } catch (IOException exception) {
                if (running.get()) logger.warning("Peer accept failed: " + exception.getClass().getSimpleName());
            }
        }
    }

    private void submitAccepted(SSLSocket socket) {
        try { connectionWorkers.execute(() -> authenticateAndHold(socket)); }
        catch (java.util.concurrent.RejectedExecutionException exception) {
            pendingSockets.remove(socket);
            closeQuietly(socket);
            logger.warning("Rejected inbound peer connection because the worker limit was reached.");
        }
    }

    private void connectWithRetry(PeerEndpoint endpoint) {
        long backoff = reconnectMinMillis;
        while (running.get()) {
            if (connections.containsKey(endpoint.serverId())) {
                sleep(deliveryPollMillis);
                continue;
            }
            Socket plainSocket = new Socket();
            pendingSockets.add(plainSocket);
            SSLSocket tlsSocket = null;
            long connectionStarted = System.nanoTime();
            try {
                plainSocket.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), handshakeTimeoutMillis);
                tlsSocket = (SSLSocket) tlsContext.getSocketFactory()
                        .createSocket(plainSocket, endpoint.host(), endpoint.port(), true);
                pendingSockets.remove(plainSocket);
                pendingSockets.add(tlsSocket);
                TlsContextFactory.configureClientSocket(tlsSocket);
                tlsSocket.setSoTimeout(handshakeTimeoutMillis);
                authenticateAndHold(tlsSocket);
                if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - connectionStarted) >= 30_000) {
                    backoff = reconnectMinMillis;
                }
            } catch (IOException | RuntimeException exception) {
                closeQuietly(tlsSocket);
                if (running.get()) logger.warning("Peer " + endpoint.serverId() + " unavailable: "
                        + exception.getClass().getSimpleName());
            } finally {
                pendingSockets.remove(plainSocket);
                closeQuietly(plainSocket);
            }
            if (!running.get()) break;
            sleep(backoff);
            backoff = Math.min(reconnectMaxMillis, backoff >= reconnectMaxMillis / 2
                    ? reconnectMaxMillis : backoff * 2);
        }
    }

    private void authenticateAndHold(SSLSocket socket) {
        String peerId = null;
        PeerSession session = null;
        try {
            socket.setSoTimeout(handshakeTimeoutMillis);
            peerId = PeerHandshake.authenticate(socket, localServerId, authorization, maxFrameBytes);
            session = new PeerSession(peerId, socket, maxFrameBytes);
            PeerSession previous = connections.putIfAbsent(peerId, session);
            if (previous != null) throw new IOException("A connection for this peer is already active");
            socket.setSoTimeout(0);
            logger.info("Authenticated peer session established: " + peerId);

            if (messageHandler != null) {
                PeerSession active = session;
                deliveryWorkers.execute(() -> deliverJournal(active));
            }

            while (running.get() && session.isOpen()) {
                byte[] frame = FrameCodec.readFrame(socket.getInputStream(), maxFrameBytes);
                session.markInbound();
                if (PeerControlMessage.isControl(frame)) {
                    if (!controlRateLimiter.tryAcquire(peerId, frame.length))
                        throw new IOException("Peer exceeded the control-message rate limit");
                    PeerControlMessage control = PeerControlMessage.decode(frame);
                    if (control.type() == PeerControlMessage.Type.PING) {
                        session.write(PeerControlMessage.encodePong(control.nonce()));
                    } else {
                        PendingHeartbeat pending = session.pendingHeartbeat.getAndSet(null);
                        if (pending == null || !pending.nonce.equals(control.nonce()))
                            throw new IOException("Unsolicited or mismatched heartbeat response");
                        session.heartbeatPongsReceived.incrementAndGet();
                    }
                    continue;
                }
                if (messageHandler == null) {
                    throw new IOException("Application messages are not enabled");
                }
                if (ReplicationMessageCodec.isAcknowledgement(frame)) {
                    PendingDelivery pending = session.pending.get();
                    if (pending == null) throw new IOException("Unsolicited replication acknowledgement");
                    ReplicationAcknowledgement acknowledgement =
                            ReplicationMessageCodec.decodeAck(frame, pending.entry.envelope().eventId());
                    pending.future.complete(acknowledgement);
                } else {
                    byte[] acknowledgement = messageHandler.handle(frame, peerId);
                    session.write(acknowledgement);
                }
            }
        } catch (IOException | RuntimeException exception) {
            if (peerId != null && running.get())
                logger.info("Peer session closed: " + peerId + " (" + exception.getClass().getSimpleName() + ")");
            else if (peerId == null)
                logger.warning("Rejected unauthenticated peer connection: " + exception.getClass().getSimpleName());
        } finally {
            if (session != null) {
                session.close();
                connections.remove(peerId, session);
            }
            pendingSockets.remove(socket);
            closeQuietly(socket);
        }
    }

    private void checkHeartbeats() {
        if (!running.get()) return;
        long now = System.nanoTime();
        for (PeerSession session : connections.values()) {
            if (!session.isOpen()) continue;
            PendingHeartbeat pending = session.pendingHeartbeat.get();
            if (pending != null) {
                if (now - pending.sentAtNanos >= TimeUnit.MILLISECONDS.toNanos(heartbeatTimeoutMillis)) {
                    logger.warning("Peer " + session.peerId + " missed its heartbeat response; closing session.");
                    session.close();
                }
                continue;
            }
            if (now - session.lastInboundNanos.get() < TimeUnit.MILLISECONDS.toNanos(heartbeatIntervalMillis))
                continue;
            PendingHeartbeat next = new PendingHeartbeat(UUID.randomUUID(), now);
            if (!session.pendingHeartbeat.compareAndSet(null, next)) continue;
            try {
                session.write(PeerControlMessage.encodePing(next.nonce));
            } catch (IOException exception) {
                session.close();
            }
        }
    }

    /** Number of valid PONG responses received on the currently active peer session. */
    public long heartbeatPongCount(String peerServerId) {
        PeerSession session = connections.get(peerServerId);
        return session == null ? 0 : session.heartbeatPongsReceived.get();
    }

    private void deliverJournal(PeerSession session) {
        while (running.get() && session.isOpen()) {
            try {
                long cursor = eventJournal.acknowledgedOffset(session.peerId);
                var entries = eventJournal.readAfter(cursor, 1);
                if (entries.isEmpty()) {
                    sleep(deliveryPollMillis);
                    continue;
                }
                JournalEntry entry = entries.get(0);
                PendingDelivery pending = new PendingDelivery(entry);
                if (!session.pending.compareAndSet(null, pending))
                    throw new IOException("A delivery is already awaiting acknowledgement");
                session.write(ReplicationMessageCodec.encodeEvent(entry.envelope()));
                ReplicationAcknowledgement acknowledgement;
                try {
                    acknowledgement = pending.future.get(acknowledgementTimeoutMillis, TimeUnit.MILLISECONDS);
                } catch (TimeoutException exception) {
                    throw new IOException("Peer acknowledgement timed out", exception);
                } catch (ExecutionException exception) {
                    throw new IOException("Peer session ended before acknowledgement", exception.getCause());
                } finally {
                    session.pending.compareAndSet(pending, null);
                }
                eventJournal.acknowledge(session.peerId, entry, acknowledgement.receiverReceiptOffset());
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                session.close();
                return;
            } catch (IOException | SQLException | RuntimeException exception) {
                if (running.get()) logger.warning("Delivery to peer " + session.peerId + " paused: "
                        + exception.getClass().getSimpleName());
                session.close();
                return;
            }
        }
    }

    private static void sleep(long millis) {
        try { Thread.sleep(millis); }
        catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
    }

    @Override public synchronized void close() {
        if (!running.getAndSet(false)) {
            connectionWorkers.shutdownNow();
            deliveryWorkers.shutdownNow();
            heartbeatWorker.shutdownNow();
            return;
        }
        closeQuietly(listener);
        listener = null;
        for (Socket socket : pendingSockets) closeQuietly(socket);
        pendingSockets.clear();
        for (PeerSession session : connections.values()) session.close();
        connections.clear();
        connectionWorkers.shutdownNow();
        deliveryWorkers.shutdownNow();
        heartbeatWorker.shutdownNow();
        try {
            connectionWorkers.awaitTermination(5, TimeUnit.SECONDS);
            deliveryWorkers.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        Thread thread = acceptThread;
        if (thread != null) thread.interrupt();
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable == null) return;
        try { closeable.close(); } catch (IOException ignored) { /* best-effort shutdown */ }
    }

    private static final class PendingDelivery {
        private final JournalEntry entry;
        private final CompletableFuture<ReplicationAcknowledgement> future = new CompletableFuture<>();
        private PendingDelivery(JournalEntry entry) { this.entry = entry; }
    }

    private static final class PendingHeartbeat {
        private final UUID nonce;
        private final long sentAtNanos;
        private PendingHeartbeat(UUID nonce, long sentAtNanos) {
            this.nonce = nonce;
            this.sentAtNanos = sentAtNanos;
        }
    }

    private static final class PeerSession {
        private final String peerId;
        private final SSLSocket socket;
        private final int maxFrameBytes;
        private final AtomicBoolean open = new AtomicBoolean(true);
        private final AtomicReference<PendingDelivery> pending = new AtomicReference<>();
        private final AtomicReference<PendingHeartbeat> pendingHeartbeat = new AtomicReference<>();
        private final AtomicLong lastInboundNanos = new AtomicLong(System.nanoTime());
        private final AtomicLong heartbeatPongsReceived = new AtomicLong();
        private PeerSession(String peerId, SSLSocket socket, int maxFrameBytes) {
            this.peerId = peerId;
            this.socket = socket;
            this.maxFrameBytes = maxFrameBytes;
        }
        private void markInbound() { lastInboundNanos.set(System.nanoTime()); }
        private boolean isOpen() { return open.get() && !socket.isClosed(); }
        private void write(byte[] frame) throws IOException {
            synchronized (socket) {
                if (!isOpen()) throw new IOException("Peer session is closed");
                FrameCodec.writeFrame(socket.getOutputStream(), frame, maxFrameBytes);
            }
        }
        private void close() {
            if (open.getAndSet(false)) {
                PendingDelivery current = pending.getAndSet(null);
                if (current != null) current.future.completeExceptionally(new IOException("Peer disconnected"));
                pendingHeartbeat.set(null);
                closeQuietly(socket);
            }
        }
    }
}