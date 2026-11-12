package dev.aemiroo.globalsync;

import dev.aemiroo.globalsync.replication.EventDeduplicator;
import dev.aemiroo.globalsync.replication.SequenceClock;
import org.bukkit.Bukkit;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.List;

public final class GlobalSyncPlugin extends JavaPlugin {
    private SequenceClock sequenceClock;
    private EventDeduplicator eventDeduplicator;
    private String serverId;
    private List<String> sharedWorlds;

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
        sequenceClock = new SequenceClock();
        eventDeduplicator = new EventDeduplicator(16_384);

        getLogger().info("GlobalSync foundation enabled for node '" + serverId + "'.");
        getLogger().info("Shared-world candidates: " + sharedWorlds);
        getLogger().warning("Replication transport is not enabled in this milestone; this node is not yet sharing world state.");
    }

    @Override
    public void onDisable() {
        getLogger().info("GlobalSync stopped.");
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
        sender.sendMessage("§7Transport: §cnot connected");
        sender.sendMessage("§7Replication: §eprimitives initialized; world replication not implemented");
        return true;
    }

    public SequenceClock sequenceClock() {
        return sequenceClock;
    }

    public EventDeduplicator eventDeduplicator() {
        return eventDeduplicator;
    }
}
