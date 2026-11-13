package dev.aemiroo.globalsync.network;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

/** Strict protocol-v2 liveness control frames, separate from durable replication events. */
public final class PeerControlMessage {
    private static final Set<String> FIELDS = Set.of("type", "protocolVersion", "nonce");

    public enum Type { PING, PONG }
    public record Message(Type type, UUID nonce) { }

    private PeerControlMessage() { }

    public static byte[] encodePing(UUID nonce) { return encode("PING", nonce); }
    public static byte[] encodePong(UUID nonce) { return encode("PONG", nonce); }

    private static byte[] encode(String type, UUID nonce) {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("protocolVersion", 2);
        json.addProperty("nonce", java.util.Objects.requireNonNull(nonce, "nonce").toString());
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static boolean isControl(byte[] payload) throws IOException {
        JsonObject json = parse(payload, FrameCodec.DEFAULT_MAX_FRAME_BYTES);
        var typeElement = json.get("type");
        if (typeElement == null || !typeElement.isJsonPrimitive()
                || !typeElement.getAsJsonPrimitive().isString()) return false;
        String type = typeElement.getAsString();
        return "PING".equals(type) || "PONG".equals(type);
    }

    public static Message decode(byte[] payload) throws IOException {
        if (payload == null || payload.length > 256)
            throw new IOException("Invalid peer control frame size");
        JsonObject json = parse(payload, 256);
        if (!json.keySet().equals(FIELDS) || !json.get("type").isJsonPrimitive()
                || !json.get("type").getAsJsonPrimitive().isString()
                || !json.get("protocolVersion").isJsonPrimitive()
                || !json.get("protocolVersion").getAsJsonPrimitive().isNumber()
                || json.get("protocolVersion").getAsInt() != 2
                || !json.get("nonce").isJsonPrimitive()
                || !json.get("nonce").getAsJsonPrimitive().isString())
            throw new IOException("Invalid peer control frame");
        try {
            String type = json.get("type").getAsString();
            Type parsedType = switch (type) {
                case "PING" -> Type.PING;
                case "PONG" -> Type.PONG;
                default -> throw new IllegalArgumentException("Unsupported control type");
            };
            UUID nonce = UUID.fromString(json.get("nonce").getAsString());
            if (!nonce.toString().equals(json.get("nonce").getAsString()))
                throw new IllegalArgumentException("Non-canonical heartbeat nonce");
            return new Message(parsedType, nonce);
        } catch (RuntimeException exception) {
            throw new IOException("Invalid peer control frame", exception);
        }
    }

    private static JsonObject parse(byte[] payload, int maxBytes) throws IOException {
        if (payload == null || payload.length < 2 || payload.length > maxBytes)
            throw new IOException("Invalid peer control frame size");
        try {
            String text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(payload)).toString();
            var parsed = JsonParser.parseString(text);
            if (!parsed.isJsonObject() || !parsed.getAsJsonObject().toString().equals(text))
                throw new IOException("Peer control frame is not canonical JSON");
            return parsed.getAsJsonObject();
        } catch (java.nio.charset.CharacterCodingException | RuntimeException exception) {
            throw new IOException("Malformed peer control frame", exception);
        }
    }
}
