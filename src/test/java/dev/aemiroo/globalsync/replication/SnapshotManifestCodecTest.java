package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotManifestCodecTest {
    private static final String SHA = "a".repeat(64);

    @Test
    void roundTripsManifestAndPreservesPerOriginAppliedWatermarks() throws Exception {
        SnapshotManifest manifest = manifest(Map.of("europe", 0L, "asia", 42L));

        SnapshotManifest decoded = SnapshotManifestCodec.decode(SnapshotManifestCodec.encode(manifest));

        assertEquals(manifest, decoded);
        assertEquals(42L, decoded.appliedThrough().get("asia"));
        assertEquals(0L, decoded.appliedThrough().get("europe"));
    }

    @Test
    void encodesOriginVectorInDeterministicSortedOrder() {
        Map<String, Long> vector = new LinkedHashMap<>();
        vector.put("zulu", 1L);
        vector.put("asia", 2L);
        SnapshotManifest manifest = manifest(vector);

        String encoded = new String(SnapshotManifestCodec.encode(manifest), StandardCharsets.UTF_8);

        assertTrue(encoded.indexOf("\"asia\"") < encoded.indexOf("\"zulu\""));
    }

    @Test
    void rejectsInvalidManifestMetadata() {
        assertThrows(IllegalArgumentException.class, () -> new SnapshotManifest(
                2, UUID.randomUUID().toString(), "world", "asia", Instant.EPOCH,
                SnapshotManifest.BLOCK_DATA_FORMAT, 1, SHA, 0, Map.of("asia", 0L)));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotManifest(
                1, UUID.randomUUID().toString(), "../world", "asia", Instant.EPOCH,
                SnapshotManifest.BLOCK_DATA_FORMAT, 1, SHA, 0, Map.of("asia", 0L)));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotManifest(
                1, UUID.randomUUID().toString(), "world", "asia", Instant.EPOCH,
                SnapshotManifest.BLOCK_DATA_FORMAT, 1, "ABC", 0, Map.of("asia", 0L)));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotManifest(
                1, UUID.randomUUID().toString(), "world", "asia", Instant.EPOCH,
                SnapshotManifest.BLOCK_DATA_FORMAT, 1, SHA, 0, Map.of("europe", 0L)));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotManifest(
                1, UUID.randomUUID().toString(), "world", "asia", Instant.EPOCH,
                SnapshotManifest.BLOCK_DATA_FORMAT, 1, SHA, 0, Map.of("asia", -1L)));
    }

    @Test
    void rejectsUnknownFieldsAndNonCanonicalJson() {
        String encoded = new String(SnapshotManifestCodec.encode(manifest(Map.of("asia", 1L))),
                StandardCharsets.UTF_8);

        assertThrows(IOException.class, () -> SnapshotManifestCodec.decode(
                encoded.replace("\"blockCount\":0", "\"blockCount\":0,\"extra\":true")
                        .getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class, () -> SnapshotManifestCodec.decode(
                (" " + encoded).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void rejectsUnsortedOriginVectors() {
        String encoded = new String(SnapshotManifestCodec.encode(manifest(Map.of("asia", 1L, "europe", 0L))),
                StandardCharsets.UTF_8);
        String reordered = encoded.replace(
                "\"appliedThrough\":{\"asia\":1,\"europe\":0}",
                "\"appliedThrough\":{\"europe\":0,\"asia\":1}");

        assertNotEquals(encoded, reordered);
        assertThrows(IOException.class,
                () -> SnapshotManifestCodec.decode(reordered.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void rejectsNegativeFractionalAndOverflowingWatermarks() {
        String encoded = new String(SnapshotManifestCodec.encode(manifest(Map.of("asia", 1L))),
                StandardCharsets.UTF_8);

        assertThrows(IOException.class, () -> SnapshotManifestCodec.decode(
                encoded.replace("\"asia\":1", "\"asia\":-1").getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class, () -> SnapshotManifestCodec.decode(
                encoded.replace("\"asia\":1", "\"asia\":1.5").getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class, () -> SnapshotManifestCodec.decode(
                encoded.replace("\"asia\":1", "\"asia\":9223372036854775808")
                        .getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void rejectsInvalidUtf8AndOversizedManifest() {
        assertThrows(IOException.class, () -> SnapshotManifestCodec.decode(new byte[]{'{', (byte) 0xff, '}'}));
        assertThrows(IOException.class,
                () -> SnapshotManifestCodec.decode(new byte[65_537]));
    }

    private static SnapshotManifest manifest(Map<String, Long> vector) {
        return new SnapshotManifest(
                SnapshotManifest.CURRENT_FORMAT_VERSION,
                "123e4567-e89b-12d3-a456-426614174000",
                "world",
                "asia",
                Instant.parse("2026-11-13T10:00:00Z"),
                SnapshotManifest.BLOCK_DATA_FORMAT,
                128,
                SHA,
                0,
                vector
        );
    }
}
