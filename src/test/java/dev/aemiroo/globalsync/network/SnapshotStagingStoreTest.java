package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.SnapshotManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotStagingStoreTest {
    @TempDir
    Path temporaryDirectory;

    private static final UUID SNAPSHOT =
            UUID.fromString("123e4567-e89b-12d3-a456-426614174001");
    private static final UUID REQUEST =
            UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    @Test
    void receivesSequentialChunksAndMarksCompleteOnlyAfterHashVerification() throws Exception {
        byte[] payload = "durable snapshot payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        SnapshotManifest manifest = manifest(payload);
        try (SnapshotStagingStore store = store()) {
            assertEquals(new SnapshotStagingStore.BeginResult(0, false),
                    store.begin("asia", REQUEST, manifest));
            assertEquals(new SnapshotStagingStore.AppendResult(1, false),
                    store.append("asia", REQUEST, SNAPSHOT, 0, payload));
            store.finish("asia", REQUEST, SNAPSHOT);
            assertTrue(store.isComplete(SNAPSHOT));
        }
    }

    @Test
    void resumeReconcilesDurablePrefixAndAcceptsOnlyIdenticalDuplicateChunks() throws Exception {
        byte[] first = new byte[SnapshotTransferMessage.CHUNK_BYTES];
        for (int i = 0; i < first.length; i++) first[i] = (byte) (i % 239);
        byte[] last = new byte[]{7, 8, 9};
        byte[] payload = new byte[first.length + last.length];
        System.arraycopy(first, 0, payload, 0, first.length);
        System.arraycopy(last, 0, payload, first.length, last.length);
        SnapshotManifest manifest = manifest(payload);

        SnapshotStagingStore firstProcess = store();
        firstProcess.begin("asia", REQUEST, manifest);
        firstProcess.append("asia", REQUEST, SNAPSHOT, 0, first);
        firstProcess.close();

        try (SnapshotStagingStore restarted = store()) {
            assertEquals(new SnapshotStagingStore.BeginResult(1, false),
                    restarted.begin("asia", REQUEST, manifest));
            assertEquals(new SnapshotStagingStore.AppendResult(1, true),
                    restarted.append("asia", REQUEST, SNAPSHOT, 0, first));
            assertThrows(IOException.class, () -> restarted.append(
                    "asia", REQUEST, SNAPSHOT, 0, new byte[SnapshotTransferMessage.CHUNK_BYTES]));
            assertEquals(new SnapshotStagingStore.AppendResult(2, false),
                    restarted.append("asia", REQUEST, SNAPSHOT, 1, last));
            restarted.finish("asia", REQUEST, SNAPSHOT);
            assertTrue(restarted.isComplete(SNAPSHOT));
        }
    }

    @Test
    void rejectsOutOfOrderChunksAndWrongPeerOrRequest() throws Exception {
        byte[] payload = new byte[]{1, 2, 3};
        SnapshotManifest manifest = manifest(payload);
        try (SnapshotStagingStore store = store()) {
            store.begin("asia", REQUEST, manifest);
            assertThrows(IOException.class, () -> store.append(
                    "asia", REQUEST, SNAPSHOT, 1, new byte[]{1}));
            assertThrows(IOException.class, () -> store.append(
                    "attacker", REQUEST, SNAPSHOT, 0, payload));
            assertThrows(IOException.class, () -> store.append(
                    "asia", UUID.randomUUID(), SNAPSHOT, 0, payload));
        }
    }

    @Test
    void refusesIncompleteAndCorruptSnapshots() throws Exception {
        byte[] payload = new byte[]{1, 2, 3};
        SnapshotManifest manifest = manifest(payload);
        SnapshotManifest wrongDigest = new SnapshotManifest(1, SNAPSHOT.toString(), "world", "asia",
                Instant.parse("2026-11-13T10:00:00Z"), SnapshotManifest.BLOCK_DATA_FORMAT,
                payload.length, "0".repeat(64), 1, Map.of("asia", 1L));
        try (SnapshotStagingStore store = store()) {
            store.begin("asia", REQUEST, manifest);
            assertThrows(IOException.class, () -> store.finish("asia", REQUEST, SNAPSHOT));
        }

        // A separate snapshot ID avoids reusing a durable row with a different manifest.
        UUID corruptId = UUID.randomUUID();
        SnapshotManifest corrupt = new SnapshotManifest(1, corruptId.toString(), "world", "asia",
                Instant.parse("2026-11-13T10:00:00Z"), SnapshotManifest.BLOCK_DATA_FORMAT,
                payload.length, wrongDigest.contentSha256(), 1, Map.of("asia", 1L));
        try (SnapshotStagingStore store = store()) {
            store.begin("asia", REQUEST, corrupt);
            store.append("asia", REQUEST, corruptId, 0, payload);
            assertThrows(IOException.class, () -> store.finish("asia", REQUEST, corruptId));
            assertFalse(store.isComplete(corruptId));
        }
    }

    @Test
    void enforcesLocalSizePolicyBeforeCreatingFiles() throws Exception {
        SnapshotManifest tooLarge = new SnapshotManifest(1, SNAPSHOT.toString(), "world", "asia",
                Instant.parse("2026-11-13T10:00:00Z"), SnapshotManifest.BLOCK_DATA_FORMAT,
                SnapshotStagingStore.MAX_SNAPSHOT_BYTES + 1, "a".repeat(64), 0, Map.of("asia", 0L));
        try (SnapshotStagingStore store = store()) {
            assertThrows(IOException.class, () -> store.begin("asia", REQUEST, tooLarge));
        }
    }

    private SnapshotStagingStore store() throws Exception {
        return new SnapshotStagingStore(temporaryDirectory.resolve("staging"));
    }

    private static SnapshotManifest manifest(byte[] payload) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload);
        return new SnapshotManifest(1, SNAPSHOT.toString(), "world", "asia",
                Instant.parse("2026-11-13T10:00:00Z"), SnapshotManifest.BLOCK_DATA_FORMAT,
                payload.length, HexFormat.of().formatHex(digest), 1, Map.of("asia", 1L));
    }
}
