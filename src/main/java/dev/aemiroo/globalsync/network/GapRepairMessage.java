package dev.aemiroo.globalsync.network;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.aemiroo.globalsync.replication.ReplicationEnvelope;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

/**
 * Strict wire representations for selective origin-sequence recovery.
 *
 * These messages are transported only inside an authenticated peer session.
 * The connection manager must additionally bind a request's origin to its
 * local server ID before looking up an event in the local journal.
 */
public final class GapRepairMessage {
    private static final Set<String> REQUEST_FIELDS =
            Set.of("type", "protocolVersion", "requestId", "originServerId", "sequence");
    private static final Set<String> EVENT_FIELDS =
            Set.of("type", "protocolVersion", "requestId", "event");
    private static final Set<String> UNAVAILABLE_FIELDS =
            Set.of("type", "protocolVersion", "requestId", "originServerId", "sequence", "reason");

    public enum Type { REQUEST, EVENT, UNAVAILABLE }
    public enum UnavailableReason { NOT_RETAINED }
    public record Message(Type type, UUID requestId, String originServerId,
                          long sequence, String eventPayload, UnavailableReason reason) { }

    private GapRepairMessage() { }

    public static byte[] encodeRequest(UUID requestId, String originServerId, long sequence) {
        validateOrigin(originServerId);
        if (sequence < 1) throw new IllegalArgumentException("sequence must be positive");
        JsonObject json = base("GAP_REQUEST", requestId);
        json.addProperty("originServerId", originServerId);
        json.addProperty("sequence", sequence);
        return bytes(json);
    }

    public static byte[] encodeEvent(UUID requestId, ReplicationEnvelope event) {
        JsonObject json = base("GAP_EVENT", requestId);
        json.addProperty("event", new String(ReplicationMessageCodec.encodeEvent(event), StandardCharsets.UTF_8));
        return bytes(json);
    }

    public static byte[] encodeUnavailable(UUID requestId, String originServerId, long sequence) {
        validateOrigin(originServerId);
        if (sequence < 1) throw new IllegalArgumentException("sequence must be positive");
        JsonObject json = base("GAP_UNAVAILABLE", requestId);
        json.addProperty("originServerId", originServerId);
        json.addProperty("sequence", sequence);
        json.addProperty("reason", UnavailableReason.NOT_RETAINED.name());
        return bytes(json);
    }

    public static boolean isGapMessage(byte[] payload) {
        return startsWith(payload, "{\"type\":\"GAP_");
    }

    public static Message decode(byte[] payload) throws IOException {
        JsonObject json = parse(payload);
        String type = string(json, "type");
        if (integer(json, "protocolVersion") != 2) throw new IOException("Unsupported gap-repair protocol version");
        UUID requestId = uuid(json, "requestId");
        try {
            return switch (type) {
                case "GAP_REQUEST" -> {
                    requireFields(json, REQUEST_FIELDS);
                    String origin = string(json, "originServerId");
                    validateOriginForDecode(origin);
                    long sequence = positiveSequence(json);
                    yield new Message(Type.REQUEST, requestId, origin, sequence, null, null);
                }
                case "GAP_EVENT" -> {
                    requireFields(json, EVENT_FIELDS);
                    String event = string(json, "event");
                    if (event.length() > FrameCodec.DEFAULT_MAX_FRAME_BYTES)
                        throw new IOException("Gap event payload exceeds the frame limit");
                    yield new Message(Type.EVENT, requestId, null, 0, event, null);
                }
                case "GAP_UNAVAILABLE" -> {
                    requireFields(json, UNAVAILABLE_FIELDS);
                    String origin = string(json, "originServerId");
                    validateOriginForDecode(origin);
                    long sequence = positiveSequence(json);
                    if (!UnavailableReason.NOT_RETAINED.name().equals(string(json, "reason")))
                        throw new IOException("Unsupported gap-unavailable reason");
                    yield new Message(Type.UNAVAILABLE, requestId, origin, sequence, null,
                            UnavailableReason.NOT_RETAINED);
                }
                default -> throw new IOException("Unsupported gap-repair message type");
            };
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid gap-repair message", exception);
        }
    }

    private static JsonObject base(String type, UUID requestId) {
        JsonObject json = new JsonObject();
        json.addProperty("type", type);
        json.addProperty("protocolVersion", 2);
        json.addProperty("requestId", java.util.Objects.requireNonNull(requestId, "requestId").toString());
        return json;
    }

    private static byte[] bytes(JsonObject json) {
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static boolean startsWith(byte[] payload, String prefix) {
        if (payload == null) return false;
        byte[] expected = prefix.getBytes(StandardCharsets.US_ASCII);
        if (payload.length < expected.length) return false;
        for (int i = 0; i < expected.length; i++) if (payload[i] != expected[i]) return false;
        return true;
    }

    private static JsonObject parse(byte[] payload) throws IOException {
        if (payload == null || payload.length < 2 || payload.length > FrameCodec.DEFAULT_MAX_FRAME_BYTES)
            throw new IOException("Invalid gap-repair frame size");
        final String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(payload)).toString();
        } catch (CharacterCodingException exception) {
            throw new IOException("Gap-repair frame is not valid UTF-8", exception);
        }
        try {
            JsonElement parsed = JsonParser.parseString(text);
            if (!parsed.isJsonObject() || !parsed.getAsJsonObject().toString().equals(text))
                throw new IOException("Gap-repair frame is not canonical JSON");
            return parsed.getAsJsonObject();
        } catch (RuntimeException exception) {
            throw new IOException("Malformed gap-repair JSON", exception);
        }
    }

    private static void requireFields(JsonObject json, Set<String> expected) throws IOException {
        if (!json.keySet().equals(expected)) throw new IOException("Unexpected or missing gap-repair fields");
    }

    private static String string(JsonObject json, String key) throws IOException {
        JsonElement value = json.get(key);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString())
            throw new IOException("Invalid string field: " + key);
        String result = value.getAsString();
        if (result.isBlank() || result.length() > FrameCodec.DEFAULT_MAX_FRAME_BYTES)
            throw new IOException("Invalid string field: " + key);
        return result;
    }

    private static int integer(JsonObject json, String key) throws IOException {
        JsonElement value = json.get(key);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber())
            throw new IOException("Invalid integer field: " + key);
        try {
            String raw = value.getAsString();
            if (!raw.matches("(0|[1-9][0-9]*)")) throw new NumberFormatException();
            return Integer.parseInt(raw);
        } catch (NumberFormatException exception) {
            throw new IOException("Invalid integer field: " + key, exception);
        }
    }

    private static long positiveSequence(JsonObject json) throws IOException {
        JsonElement value = json.get("sequence");
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber())
            throw new IOException("Invalid sequence field");
        try {
            String raw = value.getAsString();
            if (!raw.matches("[1-9][0-9]*")) throw new NumberFormatException();
            return Long.parseLong(raw);
        } catch (NumberFormatException exception) {
            throw new IOException("Invalid sequence field", exception);
        }
    }

    private static UUID uuid(JsonObject json, String key) throws IOException {
        String value = string(json, key);
        try {
            UUID parsed = UUID.fromString(value);
            if (!parsed.toString().equals(value)) throw new IllegalArgumentException();
            return parsed;
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid UUID field: " + key, exception);
        }
    }

    private static void validateOrigin(String origin) {
        if (origin == null || !origin.matches("[A-Za-z0-9._-]{1,64}"))
            throw new IllegalArgumentException("Invalid origin server ID");
    }

    private static void validateOriginForDecode(String origin) throws IOException {
        try { validateOrigin(origin); }
        catch (IllegalArgumentException exception) { throw new IOException("Invalid origin server ID", exception); }
    }
}
