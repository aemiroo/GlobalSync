package dev.aemiroo.globalsync.replication;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Strict canonical JSON encoding for snapshot manifests. */
public final class SnapshotManifestCodec {
    private static final int MAX_MANIFEST_BYTES = 65_536;
    private static final Set<String> FIELDS = Set.of(
            "type", "formatVersion", "snapshotId", "worldId", "sourceServerId",
            "createdAt", "dataFormat", "contentLengthBytes", "contentSha256",
            "blockCount", "appliedThrough");

    private SnapshotManifestCodec() { }

    public static byte[] encode(SnapshotManifest manifest) {
        java.util.Objects.requireNonNull(manifest, "manifest");
        JsonObject json = new JsonObject();
        json.addProperty("type", "SNAPSHOT_MANIFEST");
        json.addProperty("formatVersion", manifest.formatVersion());
        json.addProperty("snapshotId", manifest.snapshotId());
        json.addProperty("worldId", manifest.worldId());
        json.addProperty("sourceServerId", manifest.sourceServerId());
        json.addProperty("createdAt", manifest.createdAt().toString());
        json.addProperty("dataFormat", manifest.dataFormat());
        json.addProperty("contentLengthBytes", manifest.contentLengthBytes());
        json.addProperty("contentSha256", manifest.contentSha256());
        json.addProperty("blockCount", manifest.blockCount());
        JsonObject vector = new JsonObject();
        new TreeMap<>(manifest.appliedThrough()).forEach(vector::addProperty);
        json.add("appliedThrough", vector);
        byte[] encoded = json.toString().getBytes(StandardCharsets.UTF_8);
        if (encoded.length > MAX_MANIFEST_BYTES) {
            throw new IllegalArgumentException("Snapshot manifest exceeds the size limit");
        }
        return encoded;
    }

    public static SnapshotManifest decode(byte[] payload) throws IOException {
        JsonObject json = parse(payload);
        if (!json.keySet().equals(FIELDS)) {
            throw new IOException("Snapshot manifest has unexpected or missing fields");
        }
        if (!"SNAPSHOT_MANIFEST".equals(string(json, "type"))) {
            throw new IOException("Unsupported snapshot manifest type");
        }
        int version = integer(json, "formatVersion");
        String snapshotId = string(json, "snapshotId");
        String worldId = string(json, "worldId");
        String source = string(json, "sourceServerId");
        String createdAtText = string(json, "createdAt");
        Instant createdAt;
        try {
            createdAt = Instant.parse(createdAtText);
            if (!createdAt.toString().equals(createdAtText)) throw new IllegalArgumentException();
        } catch (RuntimeException exception) {
            throw new IOException("Invalid canonical snapshot creation timestamp", exception);
        }
        String dataFormat = string(json, "dataFormat");
        long length = nonNegativeLong(json, "contentLengthBytes");
        String sha256 = string(json, "contentSha256");
        long blocks = nonNegativeLong(json, "blockCount");
        JsonElement vectorElement = json.get("appliedThrough");
        if (vectorElement == null || !vectorElement.isJsonObject()) {
            throw new IOException("Snapshot appliedThrough must be an object");
        }
        JsonObject vectorJson = vectorElement.getAsJsonObject();
        if (vectorJson.size() > SnapshotManifest.MAX_ORIGINS) {
            throw new IOException("Snapshot origin vector exceeds the origin limit");
        }
        Map<String, Long> vector = new TreeMap<>();
        for (Map.Entry<String, JsonElement> entry : vectorJson.entrySet()) {
            JsonElement value = entry.getValue();
            if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
                throw new IOException("Snapshot origin sequence must be an integer");
            }
            try {
                String raw = value.getAsString();
                if (!raw.matches("(0|[1-9][0-9]*)")) throw new NumberFormatException();
                vector.put(entry.getKey(), Long.parseLong(raw));
            } catch (NumberFormatException exception) {
                throw new IOException("Invalid snapshot origin sequence", exception);
            }
        }
        try {
            return new SnapshotManifest(version, snapshotId, worldId, source, createdAt,
                    dataFormat, length, sha256, blocks, vector);
        } catch (RuntimeException exception) {
            throw new IOException("Invalid snapshot manifest", exception);
        }
    }

    private static JsonObject parse(byte[] payload) throws IOException {
        if (payload == null || payload.length < 2 || payload.length > MAX_MANIFEST_BYTES) {
            throw new IOException("Invalid snapshot manifest size");
        }
        final String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(payload)).toString();
        } catch (CharacterCodingException exception) {
            throw new IOException("Snapshot manifest is not valid UTF-8", exception);
        }
        try {
            JsonElement parsed = JsonParser.parseString(text);
            if (!parsed.isJsonObject() || !parsed.getAsJsonObject().toString().equals(text)) {
                throw new IOException("Snapshot manifest is not canonical JSON");
            }
            return parsed.getAsJsonObject();
        } catch (RuntimeException exception) {
            throw new IOException("Malformed snapshot manifest JSON", exception);
        }
    }

    private static String string(JsonObject json, String key) throws IOException {
        JsonElement value = json.get(key);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new IOException("Invalid snapshot manifest string: " + key);
        }
        String result = value.getAsString();
        if (result.isBlank() || result.length() > 256) {
            throw new IOException("Invalid snapshot manifest string: " + key);
        }
        return result;
    }

    private static int integer(JsonObject json, String key) throws IOException {
        long value = nonNegativeLong(json, key);
        if (value > Integer.MAX_VALUE) throw new IOException("Snapshot manifest integer is too large: " + key);
        return (int) value;
    }

    private static long nonNegativeLong(JsonObject json, String key) throws IOException {
        JsonElement value = json.get(key);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw new IOException("Invalid snapshot manifest integer: " + key);
        }
        try {
            String raw = value.getAsString();
            if (!raw.matches("(0|[1-9][0-9]*)")) throw new NumberFormatException();
            return Long.parseLong(raw);
        } catch (NumberFormatException exception) {
            throw new IOException("Invalid snapshot manifest integer: " + key, exception);
        }
    }
}
