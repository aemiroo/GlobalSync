package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.SnapshotManifest;
import dev.aemiroo.globalsync.replication.SnapshotManifestCodec;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotTransferMessageTest {
    private static final UUID REQUEST = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
    private static final UUID SNAPSHOT = UUID.fromString("123e4567-e89b-12d3-a456-426614174001");
    private static final String SHA = "b".repeat(64);

    @Test
    void roundTripsOfferAndBindsItToAuthenticatedPeerAndSharedWorld() throws Exception {
        SnapshotManifest manifest = manifest(1);
        SnapshotTransferMessage.Message decoded = SnapshotTransferMessage.decode(
                SnapshotTransferMessage.encodeOffer(REQUEST, manifest));

        assertEquals(SnapshotTransferMessage.Type.OFFER, decoded.type());
        assertEquals(manifest, decoded.manifest());
        SnapshotTransferMessage.validateOfferAuthorization(decoded, "asia", Set.of("world"));
        assertThrows(IOException.class,
                () -> SnapshotTransferMessage.validateOfferAuthorization(decoded, "attacker", Set.of("world")));
        assertThrows(IOException.class,
                () -> SnapshotTransferMessage.validateOfferAuthorization(decoded, "asia", Set.of("other")));
    }

    @Test
    void roundTripsControlMessagesAndChunkBytes() throws Exception {
        assertEquals(SnapshotTransferMessage.Type.ACCEPT,
                SnapshotTransferMessage.decode(SnapshotTransferMessage.encodeAccept(REQUEST, SNAPSHOT)).type());
        assertEquals(SnapshotTransferMessage.Type.FINISH,
                SnapshotTransferMessage.decode(SnapshotTransferMessage.encodeFinish(REQUEST, SNAPSHOT)).type());
        assertEquals(SnapshotTransferMessage.Type.COMPLETE,
                SnapshotTransferMessage.decode(SnapshotTransferMessage.encodeComplete(REQUEST, SNAPSHOT)).type());
        assertEquals(SnapshotTransferMessage.Type.CHUNK_ACK,
                SnapshotTransferMessage.decode(SnapshotTransferMessage.encodeChunkAck(REQUEST, SNAPSHOT, 0)).type());

        byte[] data = new byte[SnapshotTransferMessage.CHUNK_BYTES];
        for (int i = 0; i < data.length; i++) data[i] = (byte) (i % 251);
        SnapshotTransferMessage.Message decoded = SnapshotTransferMessage.decode(
                SnapshotTransferMessage.encodeChunk(REQUEST, SNAPSHOT, 0, data));
        assertArrayEquals(data, decoded.chunkData());
        data[0] = 99;
        assertNotEquals(data[0], decoded.chunkData()[0], "message must defensively copy caller-owned bytes");
    }

    @Test
    void rejectsWrongChunkLengthIndexAndSnapshotIdentity() throws Exception {
        SnapshotManifest manifest = manifest(SnapshotTransferMessage.CHUNK_BYTES + 3L);
        SnapshotTransferMessage.Message first = SnapshotTransferMessage.decode(
                SnapshotTransferMessage.encodeChunk(REQUEST, SNAPSHOT, 0,
                        new byte[SnapshotTransferMessage.CHUNK_BYTES]));
        SnapshotTransferMessage.validateChunkForManifest(first, manifest);

        SnapshotTransferMessage.Message wrongLast = SnapshotTransferMessage.decode(
                SnapshotTransferMessage.encodeChunk(REQUEST, SNAPSHOT, 1, new byte[2]));
        assertThrows(IOException.class,
                () -> SnapshotTransferMessage.validateChunkForManifest(wrongLast, manifest));

        SnapshotTransferMessage.Message wrongSnapshot = SnapshotTransferMessage.decode(
                SnapshotTransferMessage.encodeChunk(REQUEST, UUID.randomUUID(), 0,
                        new byte[SnapshotTransferMessage.CHUNK_BYTES]));
        assertThrows(IOException.class,
                () -> SnapshotTransferMessage.validateChunkForManifest(wrongSnapshot, manifest));
    }

    @Test
    void rejectsUnknownFieldsNonCanonicalJsonAndDuplicateKeys() {
        String valid = new String(SnapshotTransferMessage.encodeAccept(REQUEST, SNAPSHOT), StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> SnapshotTransferMessage.decode(
                (valid.substring(0, valid.length() - 1) + ",\"extra\":true}")
                        .getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class, () -> SnapshotTransferMessage.decode(
                (" " + valid).getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class, () -> SnapshotTransferMessage.decode(
                valid.replace("\"type\":\"SNAPSHOT_ACCEPT\"",
                        "\"type\":\"SNAPSHOT_ACCEPT\",\"type\":\"SNAPSHOT_ACCEPT\"")
                        .getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void rejectsInvalidChunkDataAndOutOfRangeIndexes() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> SnapshotTransferMessage.encodeChunk(REQUEST, SNAPSHOT, 0, new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> SnapshotTransferMessage.encodeChunk(REQUEST, SNAPSHOT,
                        SnapshotTransferMessage.MAX_CHUNKS, new byte[]{1}));

        String valid = new String(SnapshotTransferMessage.encodeChunk(REQUEST, SNAPSHOT, 0, new byte[]{1}),
                StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> SnapshotTransferMessage.decode(
                valid.replace("\"AQ==\"", "\"AQ\u003d\"").getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class, () -> SnapshotTransferMessage.decode(
                valid.replace("\"chunkIndex\":0", "\"chunkIndex\":-1")
                        .getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void rejectsMalformedUtf8AndOversizedFrames() {
        assertThrows(IOException.class,
                () -> SnapshotTransferMessage.decode(new byte[]{'{', (byte) 0xff, '}'}));
        assertThrows(IOException.class,
                () -> SnapshotTransferMessage.decode(new byte[FrameCodec.DEFAULT_MAX_FRAME_BYTES + 1]));
    }

    @Test
    void rejectReasonIsAllowlisted() throws Exception {
        assertEquals(SnapshotTransferMessage.RejectReason.POLICY_DENIED,
                SnapshotTransferMessage.decode(SnapshotTransferMessage.encodeReject(
                        REQUEST, SNAPSHOT, SnapshotTransferMessage.RejectReason.POLICY_DENIED)).rejectReason());
        String valid = new String(SnapshotTransferMessage.encodeReject(
                REQUEST, SNAPSHOT, SnapshotTransferMessage.RejectReason.BUSY), StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> SnapshotTransferMessage.decode(
                valid.replace("BUSY", "RUN_REMOTE_COMMAND").getBytes(StandardCharsets.UTF_8)));
    }

    private static SnapshotManifest manifest(long length) {
        return new SnapshotManifest(1, SNAPSHOT.toString(), "world", "asia",
                Instant.parse("2026-11-13T10:00:00Z"), SnapshotManifest.BLOCK_DATA_FORMAT,
                length, SHA, 1, Map.of("asia", 1L));
    }
}
