package dev.aemiroo.globalsync;

import dev.aemiroo.globalsync.event.BlockChangeEventListener;
import dev.aemiroo.globalsync.replication.BlockChangeEnvelopeFactory;
import dev.aemiroo.globalsync.replication.EventDeduplicator;
import dev.aemiroo.globalsync.replication.ReplicationEnvelope;
import dev.aemiroo.globalsync.replication.SequenceClock;
import dev.aemiroo.globalsync.replication.SqliteEventJournal;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public final class GlobalSyncPlugin extends JavaPlugin {
    private SequenceClock sequenceClock;
    private EventDeduplicator eventDeduplicator;
    private SqliteEventJournal eventJournal;
    private ExecutorService journalExecutor;
    private AtomicLong journalOffset;
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
        if (serverId.isBlank() || serverId.equalsIgnoreCase("change-me")) {
            getLogger().severe("Set a unique server-id in config.yml before enabling GlobalSync.");
            Bukkit.getPluginManager().disablePlugin(this);
            return;
        }

        sharedWorlds = List.copyOf(getConfig().getStringList("shared-worlds"));
        playerBlockCaptureEnabled = getConfig().getBoolean("capture.player-block-changes", true);

        try {
            Path journalPath = getDataFolder().toPath().resolve("journal").resolve("events.db");
            Files.createDirectories(journalPath.getParent());
            eventJournal = new SqliteEventJournal(journalPath);
            sequenceClock = new SequenceClock(eventJournal.lastSequence(serverId));
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

        eventDeduplicator = new EventDeduplicator(16_384);

        if (playerBlockCaptureEnabled) {
            getServer().getPluginManager().registerEvents(new BlockChangeEventListener(this), this);
        }

        getLogger().info("GlobalSync foundation enabled for node '" + serverId + "'.");
        getLogger().info("Shared-world candidates: " + sharedWorlds);
        getLogger().info("Local journal recovered at offset " + journalOffset.get()
                + " with node sequence " + sequenceClock.current() + ".");
        getLogger().info("Player block-change capture: " + (playerBlockCaptureEnabled ? "enabled" : "disabled"));
        getLogger().warning("Peer transport and incoming world-state application are not enabled; events remain local to this node.");
    }

    @Override
    public void onDisable() {
        if (journalExecutor != null) {
            journalExecutor.shutdown();
            try {
                if (!journalExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    getLogger().warning("Journal worker did not stop in time; leaving its connection for process shutdown.");
                    return;
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                getLogger().warning("Interrupted while waiting for the journal worker to stop.");
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
        sender.sendMessage("§7Transport: §cnot connected");
        sender.sendMessage("§7Replication: §ejournal capture only; remote application unavailable");
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

    private void closeJournal() {
        if (eventJournal == null) {
            return;
        }
        try {
            eventJournal.close();
        } catch (SQLException exception) {
            getLogger().log(Level.SEVERE, "Could not close the local event journal cleanly.", exception);
        } finally {
            eventJournal = null;
        }
    }
}
