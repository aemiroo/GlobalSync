package dev.aemiroo.globalsync.replication;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Reconciles a multi-tick chunk scan with every durable block mutation applied
 * after the scan's conservative per-origin boundary.
 *
 * <p>Chunk snapshots may already contain some of these changes. Reapplying the
 * durable winning effects is intentional and idempotent: it gives each changed
 * block the state established by the deterministic event order at reconciliation
 * time. Callers must obtain events from the durable applied-effect history and
 * must not use received or pending events.</p>
 */
public final class SnapshotReconciler {
    private static final Comparator<ReplicationEnvelope> EVENT_ORDER =
            Comparator.comparingLong(ReplicationEnvelope::logicalClock)
                    .thenComparing(ReplicationEnvelope::originServerId)
                    .thenComparingLong(ReplicationEnvelope::sequence);

    public record Result(List<SnapshotPayloadCodec.BlockRecord> records, long reconciledEvents) {
        public Result {
            records = List.copyOf(Objects.requireNonNull(records, "records"));
            if (reconciledEvents < 0) throw new IllegalArgumentException("reconciledEvents must not be negative");
        }
    }

    private record Position(int x, int y, int z) implements Comparable<Position> {
        @Override
        public int compareTo(Position other) {
            int byX = Integer.compare(x, other.x);
            if (byX != 0) return byX;
            int byY = Integer.compare(y, other.y);
            return byY != 0 ? byY : Integer.compare(z, other.z);
        }
    }

    private SnapshotReconciler() { }

    /**
     * Applies post-boundary durable effects over the scanned block state.
     * The supplied vector remains the snapshot's conservative replay boundary;
     * effects beyond it may be present in the payload and can safely replay
     * again after installation.
     */
    public static Result reconcile(
            String worldId,
            Map<String, Long> appliedThrough,
            Iterable<SnapshotPayloadCodec.BlockRecord> scannedRecords,
            Iterable<ReplicationEnvelope> appliedEvents
    ) {
        if (worldId == null || worldId.isBlank()) throw new IllegalArgumentException("worldId is required");
        Objects.requireNonNull(appliedThrough, "appliedThrough");
        Objects.requireNonNull(scannedRecords, "scannedRecords");
        Objects.requireNonNull(appliedEvents, "appliedEvents");
        if (appliedThrough.isEmpty() || appliedThrough.size() > SnapshotManifest.MAX_ORIGINS)
            throw new IllegalArgumentException("Snapshot vector must contain between 1 and "
                    + SnapshotManifest.MAX_ORIGINS + " origins");
        appliedThrough.forEach((origin, sequence) -> {
            if (origin == null || origin.isBlank() || sequence == null || sequence < 0)
                throw new IllegalArgumentException("Invalid snapshot applied-through entry");
        });

        TreeMap<Position, SnapshotPayloadCodec.BlockRecord> blocks = new TreeMap<>();
        for (SnapshotPayloadCodec.BlockRecord record : scannedRecords) {
            Objects.requireNonNull(record, "scanned block record");
            Position position = new Position(record.x(), record.y(), record.z());
            if (blocks.putIfAbsent(position, record) != null)
                throw new IllegalArgumentException("Scanned snapshot contains a duplicate block coordinate");
            if (blocks.size() > SnapshotPayloadCodec.MAX_BLOCK_RECORDS)
                throw new IllegalArgumentException("Scanned snapshot exceeds the block record limit");
        }

        List<ReplicationEnvelope> ordered = new ArrayList<>();
        Set<String> eventSequences = new HashSet<>();
        for (ReplicationEnvelope event : appliedEvents) {
            Objects.requireNonNull(event, "applied event");
            if (!worldId.equals(event.worldId()))
                throw new IllegalArgumentException("Applied event belongs to a different world");
            if (event.worldSequence() < 1)
                throw new IllegalArgumentException("Applied event has no trustworthy world-local sequence");
            long boundary = appliedThrough.getOrDefault(event.originServerId(), 0L);
            if (event.worldSequence() <= boundary) continue;
            String identity = event.originServerId() + ":" + event.worldSequence();
            if (!eventSequences.add(identity))
                throw new IllegalArgumentException("Applied event history contains a duplicate world sequence");
            ordered.add(event);
        }
        ordered.sort(EVENT_ORDER);

        for (ReplicationEnvelope event : ordered) {
            if (!BlockChangeEnvelopeFactory.BLOCK_PLACE.equals(event.eventType())
                    && !BlockChangeEnvelopeFactory.BLOCK_BREAK.equals(event.eventType())) {
                throw new IllegalArgumentException("Unsupported event type in block snapshot reconciliation");
            }
            int x = parseCoordinate(event, "x");
            int y = parseCoordinate(event, "y");
            int z = parseCoordinate(event, "z");
            String blockData = event.attributes().get("new_block_data");
            if (blockData == null) throw new IllegalArgumentException("Applied event has no resulting block data");
            Position position = new Position(x, y, z);
            if ("minecraft:air".equals(blockData)) {
                blocks.remove(position);
            } else {
                blocks.put(position, new SnapshotPayloadCodec.BlockRecord(x, y, z, blockData));
            }
            if (blocks.size() > SnapshotPayloadCodec.MAX_BLOCK_RECORDS)
                throw new IllegalArgumentException("Reconciled snapshot exceeds the block record limit");
        }
        return new Result(List.copyOf(blocks.values()), ordered.size());
    }

    private static int parseCoordinate(ReplicationEnvelope event, String name) {
        String value = event.attributes().get(name);
        if (value == null) throw new IllegalArgumentException("Applied event has no " + name + " coordinate");
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Applied event has an invalid " + name + " coordinate", exception);
        }
    }
}
