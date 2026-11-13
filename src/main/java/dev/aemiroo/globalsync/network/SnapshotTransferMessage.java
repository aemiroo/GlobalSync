package dev.aemiroo.globalsync.network;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.aemiroo.globalsync.replication.SnapshotManifest;
import dev.aemiroo.globalsync.replication.SnapshotManifestCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;
import java.util.UUID;

/**
 * Strict protocol-v2 snapshot transfer messages.
 *
 * <p>These frames are meaningful only inside a PeerConnectionManager session
 * that has completed mutual TLS and the pinned-certificate HELLO/READY exchange.
 * This codec never opens files, chooses paths, or performs world operations.</p>
 */
public final class SnapshotTransferMessage {
    public static final int CHUNK_BYTES = 256 * 1024;
    public static final long MAX_CHUNKS =
            SnapshotManifest.MAX_CONTENT_LENGTH_BYTES / CHUNK_BYTES;
    private static final Set<String> OFFER_FIELDS =
            Set.of("type", "protocolVersion", "requestId", "manifest");
    private static final Set<String> ID_FIELDS =
            Set.of("type", "protocolVersion", "requestId", "snapshotId");
    private static final Set<String> CHUNK_FIELDS =
            Set.of("type", "protocolVersion", "requestId", "snapshotId", "chunkIndex", "data");
    private static final Set<String> INDEX_FIELDS =
            Set.of("type", "protocolVersion", "requestId", "snapshotId", "chunkIndex");
    private static final Set<String> REJECT_FIELDS =
            Set.of("type", "protocolVersion", "requestId", "snapshotId", "reason");
    private static final int MAX_FRAME_BYTES = FrameCodec.DEFAULT_MAX_FRAME_BYTES;

    public enum Type { OFFER, ACCEPT, REJECT, CHUNK, CHUNK_ACK, FINISH, COMPLETE }
    public enum RejectReason { BUSY, WORLD_NOT_SHARED, UNSUPPORTED_FORMAT, POLICY_DENIED }
    public record Message(Type type, UUID requestId, UUID snapshotId,
                          SnapshotManifest manifest, long chunkIndex,
                          byte[] chunkData, RejectReason rejectReason) {
        public Message {
            java.util.Objects.requireNonNull(type, "type");
            java.util.Objects.requireNonNull(requestId, "requestId");
            if (chunkData != null) chunkData = chunkData.clone();
        }
        @Override public byte[] chunkData() {
            return chunkData == null ? null : chunkData.clone();
        }
    }

    private SnapshotTransferMessage() { }

    public static byte[] encodeOffer(UUID requestId, SnapshotManifest manifest) {
        java.util.Objects.requireNonNull(manifest, "manifest");
        JsonObject json = base("SNAPSHOT_OFFER", requestId);
        json.addProperty("manifest", new String(SnapshotManifestCodec.encode(manifest), StandardCharsets.UTF_8));
        return bytes(json);
    }

    public static byte[] encodeAccept(UUID requestId, UUID snapshotId) {
        return encodeId("SNAPSHOT_ACCEPT", requestId, snapshotId);
    }

    public static byte[] encodeFinish(UUID requestId, UUID snapshotId) {
        return encodeId("SNAPSHOT_FINISH", requestId, snapshotId);
    }

    public static byte[] encodeComplete(UUID requestId, UUID snapshotId) {
        return encodeId("SNAPSHOT_COMPLETE", requestId, snapshotId);
    }

    public static byte[] encodeChunk(UUID requestId, UUID snapshotId, long chunkIndex, byte[] data) {
        validateChunkIndex(chunkIndex);
        if (data == null || data.length < 1 || data.length > CHUNK_BYTES)
            throw new IllegalArgumentException("Snapshot chunk size is outside the allowed range");
        JsonObject json = base("SNAPSHOT_CHUNK", requestId);
        json.addProperty("snapshotId", requireUuid(snapshotId).toString());
        json.addProperty("chunkIndex", chunkIndex);
        json.addProperty("data", Base64.getEncoder().encodeToString(data));
        return bytes(json);
    }

    public static byte[] encodeChunkAck(UUID requestId, UUID snapshotId, long chunkIndex) {
        validateChunkIndex(chunkIndex);
        JsonObject json = base("SNAPSHOT_CHUNK_ACK", requestId);
        json.addProperty("snapshotId", requireUuid(snapshotId).toString());
        json.addProperty("chunkIndex", chunkIndex);
        return bytes(json);
    }

    public static byte[] encodeReject(UUID requestId, UUID snapshotId, RejectReason reason) {
        JsonObject json = base("SNAPSHOT_REJECT", requestId);
        json.addProperty("snapshotId", requireUuid(snapshotId).toString());
        json.addProperty("reason", java.util.Objects.requireNonNull(reason, "reason").name());
        return bytes(json);
    }

    public static boolean isSnapshotMessage(byte[] payload) {
        return startsWith(payload, "{\"type\":\"SNAPSHOT_");
    }

    /**
     * Binds an offer to the already authenticated peer identity and local
     * shared-world allowlist. Certificate authentication alone is not enough.
     */
    public static void validateOfferAuthorization(Message message, String authenticatedPeerId,
                                                  Set<String> sharedWorlds) throws IOException {
        if (message == null || message.type() != Type.OFFER || message.manifest() == null)
            throw new IOException("Expected a snapshot offer");
        if (authenticatedPeerId == null
                || !authenticatedPeerId.equals(message.manifest().sourceServerId()))
            throw new IOException("Snapshot source does not match the authenticated peer");
        if (sharedWorlds == null || !sharedWorlds.contains(message.manifest().worldId()))
            throw new IOException("Snapshot world is not configured for sharing");
    }

    public static Message decode(byte[] payload) throws IOException {
        JsonObject json = parse(payload);
        String type = string(json, "type");
        if (integer(json, "protocolVersion") != 2)
            throw new IOException("Unsupported snapshot-transfer protocol version");
        UUID requestId = uuid(json, "requestId");
        try {
            return switch (type) {
                case "SNAPSHOT_OFFER" -> {
                    requireFields(json, OFFER_FIELDS);
                    SnapshotManifest manifest = SnapshotManifestCodec.decode(
                            string(json, "manifest").getBytes(StandardCharsets.UTF_8));
                    yield new Message(Type.OFFER, requestId, UUID.fromString(manifest.snapshotId()),
                            manifest, -1, null, null);
                }
                case "SNAPSHOT_ACCEPT" -> decodeId(json, requestId, Type.ACCEPT, ID_FIELDS);
                case "SNAPSHOT_FINISH" -> decodeId(json, requestId, Type.FINISH, ID_FIELDS);
                case "SNAPSHOT_COMPLETE" -> decodeId(json, requestId, Type.COMPLETE, ID_FIELDS);
                case "SNAPSHOT_CHUNK" -> {
                    requireFields(json, CHUNK_FIELDS);
                    long index = nonNegativeLong(json, "chunkIndex");
                    validateChunkIndexForDecode(index);
                    String encoded = string(json, "data");
                    if (encoded.length() > ((CHUNK_BYTES + 2) / 3) * 4)
                        throw new IOException("Snapshot chunk exceeds the encoded size limit");
                    final byte[] data;
                    try {
                        data = Base64.getDecoder().decode(encoded);
                    } catch (IllegalArgumentException exception) {
                        throw new IOException("Snapshot chunk is not valid Base64", exception);
                    }
                    if (data.length < 1 || data.length > CHUNK_BYTES
                            || !Base64.getEncoder().encodeToString(data).equals(encoded))
                        throw new IOException("Snapshot chunk is not canonical or has an invalid size");
                    yield new Message(Type.CHUNK, requestId, uuid(json, "snapshotId"),
                            null, index, data, null);
                }
                case "SNAPSHOT_CHUNK_ACK" -> {
                    requireFields(json, INDEX_FIELDS);
                    long index = nonNegativeLong(json, "chunkIndex");
                    validateChunkIndexForDecode(index);
                    yield new Message(Type.CHUNK_ACK, requestId, uuid(json, "snapshotId"),
                            null, index, null, null);
                }
                case "SNAPSHOT_REJECT" -> {
                    requireFields(json, REJECT_FIELDS);
                    RejectReason reason;
                    try { reason = RejectReason.valueOf(string(json, "reason")); }
                    catch (IllegalArgumentException exception) {
                        throw new IOException("Unsupported snapshot rejection reason", exception);
                    }
                    yield new Message(Type.REJECT, requestId, uuid(json, "snapshotId"),
                            null, -1, null, reason);
                }
                default -> throw new IOException("Unsupported snapshot-transfer message type");
            };
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid snapshot-transfer message", exception);
        }
    }

    private static Message decodeId(JsonObject json, UUID requestId, Type type, Set<String> fields)
            throws IOException {
        requireFields(json, fields);
        return new Message(type, requestId, uuid(json, "snapshotId"), null, -1, null, null);
    }

    private static JsonObject base(String type, UUID requestId) {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("protocolVersion", 2);
        json.addProperty("requestId", requireUuid(requestId).toString());
        return json;
    }

    private static byte[] encodeId(String type, UUID requestId, UUID snapshotId) {
        JsonObject json = base(type, requestId);
        json.addProperty("snapshotId", requireUuid(snapshotId).toString());
        return bytes(json);
    }

    private static byte[] bytes(JsonObject json) {
        byte[] result = json.toString().getBytes(StandardCharsets.UTF_8);
        if (result.length > MAX_FRAME_BYTES)
            throw new IllegalArgumentException("Snapshot-transfer message exceeds the frame limit");
        return result;
    }

    private static JsonObject parse(byte[] payload) throws IOException {
        if (payload == null || payload.length < 2 || payload.length > MAX_FRAME_BYTES)
            throw new IOException("Invalid snapshot-transfer frame size");
        final String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(payload)).toString();
        } catch (CharacterCodingException exception) {
            throw new IOException("Snapshot-transfer frame is not valid UTF-8", exception);
        }
        try {
            JsonElement parsed = JsonParser.parseString(text);
            if (!parsed.isJsonObject() || !parsed.getAsJsonObject().toString().equals(text))
                throw new IOException("Snapshot-transfer frame is not canonical JSON");
            return parsed.getAsJsonObject();
        } catch (RuntimeException exception) {
            throw new IOException("Malformed snapshot-transfer JSON", exception);
        }
    }

    private static void requireFields(JsonObject json, Set<String> fields) throws IOException {
        if (!json.keySet().equals(fields))
            throw new IOException("Unexpected or missing snapshot-transfer fields");
    }

    private static String string(JsonObject json, String key) throws IOException {
        JsonElement value = json.get(key);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString())
            throw new IOException("Invalid snapshot-transfer string: " + key);
        String result = value.getAsString();
        if (result.isBlank() || result.length() > MAX_FRAME_BYTES)
            throw new IOException("Invalid snapshot-transfer string: " + key);
        return result;
    }

    private static int integer(JsonObject json, String key) throws IOException {
        long value = nonNegativeLong(json, key);
        if (value > Integer.MAX_VALUE) throw new IOException("Snapshot-transfer integer is too large");
        return (int) value;
    }

    private static long nonNegativeLong(JsonObject json, String key) throws IOException {
        JsonElement value = json.get(key);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber())
            throw new IOException("Invalid snapshot-transfer integer: " + key);
        try {
            String raw = value.getAsString();
            if (!raw.matches("(0|[1-9][0-9]*)")) throw new NumberFormatException();
            return Long.parseLong(raw);
        } catch (NumberFormatException exception) {
            throw new IOException("Invalid snapshot-transfer integer: " + key, exception);
        }
    }

    private static UUID uuid(JsonObject json, String key) throws IOException {
        String raw = string(json, key);
        try {
            UUID value = UUID.fromString(raw);
            if (!value.toString().equals(raw)) throw new IllegalArgumentException();
            return value;
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid snapshot-transfer UUID: " + key, exception);
        }
    }

    private static UUID requireUuid(UUID value) {
        return java.util.Objects.requireNonNull(value, "UUID");
    }

    private static void validateChunkIndex(long index) {
        if (index < 0 || index >= MAX_CHUNKS)
            throw new IllegalArgumentException("Snapshot chunk index is outside the supported range");
    }

    private static void validateChunkIndexForDecode(long index) throws IOException {
        if (index < 0 || index >= MAX_CHUNKS)
            throw new IOException("Snapshot chunk index is outside the supported range");
    }

    private static boolean startsWith(byte[] payload, String prefix) {
        if (payload == null) return false;
        byte[] expected = prefix.getBytes(StandardCharsets.US_ASCII);
        if (payload.length < expected.length) return false;
        for (int i = 0; i < expected.length; i++) if (payload[i] != expected[i]) return false;
        return true;
    }
}
