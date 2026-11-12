package dev.aemiroo.globalsync.event;

import dev.aemiroo.globalsync.GlobalSyncPlugin;
import dev.aemiroo.globalsync.replication.BlockChangeEnvelopeFactory;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockMultiPlaceEvent;
import org.bukkit.event.block.BlockPlaceEvent;

/**
 * Captures successful player-initiated block edits after other plugins have had
 * an opportunity to cancel them. This listener never mutates the world or event.
 */
public final class BlockChangeEventListener implements Listener {
    private static final String AIR_BLOCK_DATA = "minecraft:air";

    private final GlobalSyncPlugin plugin;

    public BlockChangeEventListener(GlobalSyncPlugin plugin) {
        this.plugin = plugin;
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPlace(BlockPlaceEvent event) {
        if (event instanceof BlockMultiPlaceEvent multiPlaceEvent) {
            captureMultiPlace(multiPlaceEvent);
            return;
        }

        Block block = event.getBlockPlaced();
        capture(
                BlockChangeEnvelopeFactory.BLOCK_PLACE,
                event.getPlayer().getUniqueId(),
                block,
                event.getBlockReplacedState().getBlockData().getAsString(),
                block.getBlockData().getAsString()
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBreak(BlockBreakEvent event) {
        Block block = event.getBlock();
        capture(
                BlockChangeEnvelopeFactory.BLOCK_BREAK,
                event.getPlayer().getUniqueId(),
                block,
                block.getBlockData().getAsString(),
                AIR_BLOCK_DATA
        );
    }

    private void captureMultiPlace(BlockMultiPlaceEvent event) {
        for (BlockState replacedState : event.getReplacedBlockStates()) {
            Block block = replacedState.getBlock();
            capture(
                    BlockChangeEnvelopeFactory.BLOCK_PLACE,
                    event.getPlayer().getUniqueId(),
                    block,
                    replacedState.getBlockData().getAsString(),
                    block.getBlockData().getAsString()
            );
        }
    }

    private void capture(
            String eventType,
            java.util.UUID playerId,
            Block block,
            String previousBlockData,
            String newBlockData
    ) {
        String worldId = block.getWorld().getName();
        if (!plugin.isSharedWorld(worldId)) {
            return;
        }

        plugin.recordPlayerBlockChange(
                eventType,
                worldId,
                playerId,
                block.getX(),
                block.getY(),
                block.getZ(),
                previousBlockData,
                newBlockData
        );
    }
}
