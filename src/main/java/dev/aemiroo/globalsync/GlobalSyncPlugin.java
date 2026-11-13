package dev.aemiroo.globalsync;

import dev.aemiroo.globalsync.event.BlockChangeEventListener;
import dev.aemiroo.globalsync.network.PeerAuthorizationRegistry;
import dev.aemiroo.globalsync.network.PeerConnectionManager;
import dev.aemiroo.globalsync.network.PeerEndpoint;
import dev.aemiroo.globalsync.network.ReplicationMessageHandler;
import dev.aemiroo.globalsync.network.TlsContextFactory;
import dev.aemiroo.globalsync.replication.BlockChangeEnvelopeFactory;
import dev.aemiroo.globalsync.replication.BlockApplicationService;
import dev.aemiroo.globalsync.replication.LogicalClock;
import dev.aemiroo.globalsync.replication.EventDeduplicator;
import dev.aemiroo.globalsync.replication.ReplicationEnvelope;
import dev.aemiroo.globalsync.replication.SequenceClock;
import dev.aemiroo.globalsync.replication.SqliteEventJournal;
import dev.aemiroo.globalsync.replication.SqliteReceivedEventStore;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.plugin.java.JavaPlugin;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public final class GlobalSyncPlugin extends JavaPlugin {
    private SequenceClock sequenceClock;
    private LogicalClock logicalClock;
    private BlockApplicationService blockApplicationService;
    private EventDeduplicator eventDeduplicator;
    private SqliteEventJournal eventJournal;
    private SqliteReceivedEventStore receivedEventStore;
    private ExecutorService journalExecutor;
    private AtomicLong journalOffset;
    private PeerConnectionManager peerConnectionManager;
    private String serverId;
    private List<String> sharedWorlds;
    private boolean playerBlockCaptureEnabled;

    @Override
    public void onEnable() {
        saveDefaultConfig();

        String minecraftVersion = Bukkit.getMinecraftVersion();
        if (!"26.3".equals(minecraftVersion)) {
            getLogger().severe("GlobalSync only supports Minecraft 26.3. Detected: " + minecraftVersion);
            Bukkit.getPluginManager().disablePlugin(this);
            return;
        }

        serverId = getConfig().getString("server-id", "").trim();
        if (!serverId.matches("[A-Za-z0-9._-]{1,64}") || serverId.equalsIgnoreCase("change-me")) {
            getLogger().severe("Set a unique server-id using letters, digits, dot, underscore or hyphen (1-64 characters).");
            Bukkit.getPluginManager().disablePlugin(this);
            return;
        }

        sharedWorlds = List.copyOf(getConfig().getStringList("shared-worlds"));
        playerBlockCaptureEnabled = getConfig().getBoolean("capture.player-block-changes", true);

        try {
            Path journalPath = getDataFolder().toPath().resolve("journal").resolve("events.db");
            Files.createDirectories(journalPath.getParent());
            eventJournal = new SqliteEventJournal(journalPath);
            Path receivedPath = journalPath.getParent().resolve("received.db");
            receivedEventStore = new SqliteReceivedEventStore(receivedPath);
            sequenceClock = new SequenceClock(eventJournal.lastSequence(serverId));
            logicalClock = new LogicalClock(Math.max(
                    eventJournal.lastLogicalClock(), receivedEventStore.lastLogicalClock()));
            restoreLocalWinnerIndex();
            if (getConfig().getBoolean("application.enabled", false)) {
                blockApplicationService = new BlockApplicationService(this, receivedEventStore);
            }
            journalOffset = new AtomicLong(eventJournal.lastOffset());
            journalExecutor = Executors.newSingleThreadExecutor(task -> {
                Thread thread = new Thread(task, "GlobalSync-Journal-" + serverId);
                thread.setDaemon(true);
                return thread;
            });
        } catch (IOException | SQLException exception) {
            getLogger().log(Level.SEVERE, "Could not open the local event journal.", exception);
            closeJournal();
            Bukkit.getPluginManager().disablePlugin(this);
            return;
        }

        try {
            startPeerNetworkingIfEnabled();
            if (blockApplicationService != null) blockApplicationService.start();
        } catch (IOException | GeneralSecurityException | RuntimeException exception) {
            getLogger().log(Level.SEVERE,
                    "Peer networking configuration or startup failed. GlobalSync will fail closed.", exception);
            closePeerNetworking();
            closeJournal();
            Bukkit.getPluginManager().disablePlugin(this);
            return;
        }

        eventDeduplicator = new EventDeduplicator(16_384);
        if (playerBlockCaptureEnabled) {
            getServer().getPluginManager().registerEvents(new BlockChangeEventListener(this), this);
        }

        getLogger().info("GlobalSync foundation enabled for node '" + serverId + "'.");
        getLogger().info("Shared-world candidates: " + sharedWorlds);
        getLogger().info("Local journal recovered at offset " + journalOffset.get()
                + " with node sequence " + sequenceClock.current() + ".");
        getLogger().info("Player block-change capture: " + (playerBlockCaptureEnabled ? "enabled" : "disabled"));
        if (peerConnectionManager == null) {
            getLogger().info("Peer networking is disabled.");
        } else {
            getLogger().info("Authenticated peers are connected; outbound delivery, durable inbox, and configured block application are active.");
        }
    }

    @Override
    public void onDisable() {
        if (blockApplicationService != null) blockApplicationService.close();
        closePeerNetworking();
        if (journalExecutor != null) {
            journalExecutor.shutdown();
            try {
                if (!journalExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    getLogger().warning("Journal worker did not stop in time; leaving the journal connection for process shutdown.");
                    journalExecutor.shutdownNow();
                    return;
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                journalExecutor.shutdownNow();
                getLogger().warning("Interrupted while waiting for the journal worker to stop; leaving the journal connection for process shutdown.");
                return;
            }
        }
        closeJournal();
        getLogger().info("GlobalSync stopped.");
    }

    /**
     * Queues a local event for durable storage without performing disk I/O on the
     * Paper main thread. The returned future completes only after SQLite commits it.
     */
    public CompletableFuture<Long> appendEvent(ReplicationEnvelope envelope) {
        if (eventJournal == null || journalExecutor == null || journalExecutor.isShutdown()) {
            return CompletableFuture.failedFuture(new IllegalStateException("The event journal is not available"));
        }
        if (!serverId.equals(envelope.originServerId())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Event origin does not match this node"));
        }
        if (!sharedWorlds.contains(envelope.worldId())) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("World is not configured for replication"));
        }

        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    long offset = eventJournal.append(envelope);
                    receivedEventStore.indexLocalJournalEntry(envelope, offset);
                    journalOffset.accumulateAndGet(offset, Math::max);
                    return offset;
                } catch (SQLException exception) {
                    throw new CompletionException(exception);
                }
            }, journalExecutor);
        } catch (java.util.concurrent.RejectedExecutionException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }

    /**
     * Called from synchronous Bukkit block events. All Bukkit values are converted
     * to plain immutable strings before the journal worker touches the event.
     */
    public void recordPlayerBlockChange(
            String eventType,
            String worldId,
            java.util.UUID playerId,
            int x,
            int y,
            int z,
            String previousBlockData,
            String newBlockData
    ) {
        if (!playerBlockCaptureEnabled || sequenceClock == null || !sharedWorlds.contains(worldId)) {
            return;
        }

        ReplicationEnvelope envelope = BlockChangeEnvelopeFactory.create(
                eventType,
                worldId,
                serverId,
                sequenceClock.next(),
                logicalClock.tick(),
                playerId,
                x,
                y,
                z,
                previousBlockData,
                newBlockData
        );

        appendEvent(envelope).whenComplete((offset, failure) -> {
            if (failure != null) {
                getLogger().log(Level.SEVERE, "Could not persist captured block event " + envelope.eventId(), failure);
            } else {
                getLogger().fine("Persisted " + eventType + " at journal offset " + offset);
            }
        });
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (args.length != 1 || !args[0].equalsIgnoreCase("status")) {
            sender.sendMessage("§6Usage: /globalsync status");
            return true;
        }

        sender.sendMessage("§6GlobalSync §7- §f26.3 foundation");
        sender.sendMessage("§7Node: §f" + (serverId == null ? "not initialized" : serverId));
        sender.sendMessage("§7Shared worlds: §f" + (sharedWorlds == null ? "[]" : sharedWorlds));
        sender.sendMessage("§7Journal: §a" + (eventJournal == null ? "unavailable" : "ready")
                + " §7(offset " + (journalOffset == null ? 0 : journalOffset.get()) + ")");
        sender.sendMessage("§7Block capture: §f" + (playerBlockCaptureEnabled ? "enabled" : "disabled"));
        sender.sendMessage("§7Peer transport: §f" + (peerConnectionManager == null
                ? "disabled" : peerConnectionManager.connectedPeerCount() + " authenticated peer(s)"));
        sender.sendMessage("§7Replication: §e" + (peerConnectionManager == null
                ? "journal capture only" : "bidirectional durable event delivery enabled"));
        sender.sendMessage("§7World application: §f" + (blockApplicationService == null
                ? "disabled" : "enabled"));
        return true;
    }

    public SequenceClock sequenceClock() {
        return sequenceClock;
    }

    public EventDeduplicator eventDeduplicator() {
        return eventDeduplicator;
    }

    public boolean isSharedWorld(String worldId) {
        return sharedWorlds != null && sharedWorlds.contains(worldId);
    }

    private void restoreLocalWinnerIndex() throws SQLException {
        long cursor = receivedEventStore.lastIndexedLocalJournalOffset();
        while (true) {
            List<dev.aemiroo.globalsync.replication.JournalEntry> entries = eventJournal.readAfter(cursor, 500);
            if (entries.isEmpty()) break;
            for (var entry : entries) {
                receivedEventStore.indexLocalJournalEntry(entry.envelope(), entry.offset());
                cursor = entry.offset();
            }
        }
    }

    private void startPeerNetworkingIfEnabled() throws IOException, GeneralSecurityException {
        ConfigurationSection network = getConfig().getConfigurationSection("network");
        if (network == null || !network.getBoolean("enabled", false)) {
            return;
        }

        ConfigurationSection peersSection = network.getConfigurationSection("peers");
        if (peersSection == null) {
            throw new IllegalArgumentException("network.peers must contain at least one authorized peer");
        }
        List<PeerEndpoint> peers = new ArrayList<>();
        for (String peerId : peersSection.getKeys(false)) {
            ConfigurationSection peer = peersSection.getConfigurationSection(peerId);
            if (peer == null) {
                throw new IllegalArgumentException("Peer configuration must be a section: " + peerId);
            }
            if (serverId.equals(peerId)) {
                throw new IllegalArgumentException("A node cannot authorize itself as a remote peer");
            }
            peers.add(new PeerEndpoint(
                    peerId,
                    peer.getString("host", ""),
                    peer.getInt("port", -1),
                    peer.getString("fingerprint", "")
            ));
        }
        if (peers.isEmpty()) {
            throw new IllegalArgumentException("network.peers must contain at least one authorized peer");
        }

        ConfigurationSection tls = network.getConfigurationSection("tls");
        if (tls == null) {
            throw new IllegalArgumentException("network.tls configuration is required");
        }
        String identityPasswordEnv = tls.getString("identity-store-password-env", "");
        String trustPasswordEnv = tls.getString("trust-store-password-env", "");
        char[] identityPassword = null;
        char[] trustPassword = null;
        SSLContext tlsContext;
        try {
            identityPassword = requireEnvironmentSecret(identityPasswordEnv);
            trustPassword = requireEnvironmentSecret(trustPasswordEnv);
            Path identityStore = resolveTlsStore(tls.getString("identity-store", ""));
            Path trustStore = resolveTlsStore(tls.getString("trust-store", ""));
            tlsContext = TlsContextFactory.create(identityStore, identityPassword, trustStore, trustPassword);
        } finally {
            if (identityPassword != null) {
                Arrays.fill(identityPassword, '\0');
            }
            if (trustPassword != null) {
                Arrays.fill(trustPassword, '\0');
            }
        }

        int maxFrameBytes = network.getInt("max-frame-bytes", 1_048_576);
        int maxConnections = network.getInt("max-connections", 16);
        int handshakeTimeoutMillis = network.getInt("handshake-timeout-ms", 5_000);
        int acknowledgementTimeoutMillis = network.getInt("acknowledgement-timeout-ms", 10_000);
        int deliveryPollMillis = network.getInt("delivery-poll-ms", 250);
        int reconnectMinMillis = network.getInt("reconnect-min-ms", 1_000);
        int reconnectMaxMillis = network.getInt("reconnect-max-ms", 30_000);
        PeerAuthorizationRegistry authorization = new PeerAuthorizationRegistry(peers);
        peerConnectionManager = new PeerConnectionManager(
                tlsContext,
                serverId,
                authorization,
                maxFrameBytes,
                maxConnections,
                handshakeTimeoutMillis,
                getLogger(),
                new ReplicationMessageHandler(receivedEventStore, Set.copyOf(sharedWorlds),
                        logicalClock, event -> {
                            if (blockApplicationService != null) blockApplicationService.requestApply(event);
                        }),
                eventJournal,
                acknowledgementTimeoutMillis,
                deliveryPollMillis,
                reconnectMinMillis,
                reconnectMaxMillis
        );
        InetAddress bindAddress = InetAddress.getByName(network.getString("bind-address", "127.0.0.1"));
        int bindPort = network.getInt("bind-port", 28_120);
        peerConnectionManager.start(bindAddress, bindPort);
        peerConnectionManager.connectConfiguredPeers();
        getLogger().info("Peer listener started on " + bindAddress.getHostAddress() + ":" + bindPort
                + " with " + peers.size() + " explicitly authorized peer(s).");
    }

    private Path resolveTlsStore(String configuredPath) throws IOException {
        if (configuredPath == null || configuredPath.isBlank()) {
            throw new IllegalArgumentException("TLS store path must not be blank");
        }
        Path dataFolder = getDataFolder().toPath().toAbsolutePath().normalize();
        Path configured = Path.of(configuredPath);
        if (configured.isAbsolute()) {
            throw new IllegalArgumentException("TLS stores must be inside the GlobalSync data directory");
        }
        Path resolved = dataFolder.resolve(configured).normalize();
        if (!resolved.startsWith(dataFolder)) {
            throw new IllegalArgumentException("TLS store path escapes the GlobalSync data directory");
        }
        Path realDataFolder = dataFolder.toRealPath();
        Path realStore = resolved.toRealPath();
        if (!realStore.startsWith(realDataFolder) || !Files.isRegularFile(realStore)) {
            throw new IllegalArgumentException("TLS store must be a regular file inside the GlobalSync data directory");
        }
        return realStore;
    }

    private static char[] requireEnvironmentSecret(String variableName) {
        if (variableName == null || !variableName.matches("[A-Z_][A-Z0-9_]{0,127}")) {
            throw new IllegalArgumentException("TLS password environment variable name is invalid");
        }
        String value = System.getenv(variableName);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Required TLS password environment variable is not set: " + variableName);
        }
        return value.toCharArray();
    }

    private void closePeerNetworking() {
        if (peerConnectionManager != null) {
            peerConnectionManager.close();
            peerConnectionManager = null;
        }
    }

    private void closeJournal() {
        if (receivedEventStore != null) {
            try {
                receivedEventStore.close();
            } catch (SQLException exception) {
                getLogger().log(Level.SEVERE, "Could not close the received-event store cleanly.", exception);
            } finally {
                receivedEventStore = null;
            }
        }
        if (eventJournal != null) {
            try {
                eventJournal.close();
            } catch (SQLException exception) {
                getLogger().log(Level.SEVERE, "Could not close the local event journal cleanly.", exception);
            } finally {
                eventJournal = null;
            }
        }
    }
}
