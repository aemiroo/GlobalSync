package dev.aemiroo.globalsync.replication;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Immutable description of one world snapshot and the contiguous applied event
 * prefix represented by its contents.
 *
 * <p>The origin vector is not a global journal offset. Each value is the highest
 * contiguous sequence from that origin whose effects are represented by the
 * snapshot. A zero value means that no event from that origin is included.</p>
 */
public record SnapshotManifest(
        int formatVersion,
        String snapshotId,
        String worldId,
        String sourceServerId,
        Instant createdAt,
        String dataFormat,
        long contentLengthBytes,
        String contentSha256,
        long blockCount,
        Map<String, Long> appliedThrough
) {
    public static final int CURRENT_FORMAT_VERSION = 1;
    public static final String BLOCK_DATA_FORMAT = "PAPER_BLOCK_DATA_V1";
    public static final int MAX_ORIGINS = 64;
    public static final long MAX_CONTENT_LENGTH_BYTES = 64L * 1024 * 1024 * 1024;
    private static final Pattern SERVER_ID = Pattern.compile("[A-Za-z0-9._-]{1,64}");
    private static final Pattern WORLD_ID = Pattern.compile("[A-Za-z0-9._:-]{1,128}");
    private static final Pattern SHA256 = Pattern.compile("[a-f0-9]{64}");

    public SnapshotManifest {
        if (formatVersion != CURRENT_FORMAT_VERSION) {
            throw new IllegalArgumentException("Unsupported snapshot format version");
        }
        try {
            if (!java.util.UUID.fromString(Objects.requireNonNull(snapshotId, "snapshotId"))
                    .toString().equals(snapshotId)) {
                throw new IllegalArgumentException("snapshotId must be a canonical UUID");
            }
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("snapshotId must be a canonical UUID", exception);
        }
        if (worldId == null || !WORLD_ID.matcher(worldId).matches()) {
            throw new IllegalArgumentException("Invalid snapshot world ID");
        }
        if (sourceServerId == null || !SERVER_ID.matcher(sourceServerId).matches()) {
            throw new IllegalArgumentException("Invalid snapshot source server ID");
        }
        Objects.requireNonNull(createdAt, "createdAt");
        if (!BLOCK_DATA_FORMAT.equals(dataFormat)) {
            throw new IllegalArgumentException("Unsupported snapshot data format");
        }
        if (contentLengthBytes < 1 || contentLengthBytes > MAX_CONTENT_LENGTH_BYTES) {
            throw new IllegalArgumentException("Snapshot content length is outside the supported range");
        }
        if (contentSha256 == null || !SHA256.matcher(contentSha256).matches()) {
            throw new IllegalArgumentException("Snapshot SHA-256 must be 64 lowercase hexadecimal characters");
        }
        if (blockCount < 0) {
            throw new IllegalArgumentException("blockCount must not be negative");
        }
        Objects.requireNonNull(appliedThrough, "appliedThrough");
        if (appliedThrough.isEmpty() || appliedThrough.size() > MAX_ORIGINS) {
            throw new IllegalArgumentException("Snapshot origin vector must contain between 1 and "
                    + MAX_ORIGINS + " origins");
        }
        TreeMap<String, Long> sorted = new TreeMap<>();
        appliedThrough.forEach((origin, sequence) -> {
            if (origin == null || !SERVER_ID.matcher(origin).matches()) {
                throw new IllegalArgumentException("Invalid origin server ID in snapshot vector");
            }
            if (sequence == null || sequence < 0) {
                throw new IllegalArgumentException("Snapshot origin sequence must not be negative");
            }
            sorted.put(origin, sequence);
        });
        if (!sorted.containsKey(sourceServerId)) {
            throw new IllegalArgumentException("Snapshot origin vector must include its source server");
        }
        appliedThrough = Collections.unmodifiableMap(sorted);
    }
}
