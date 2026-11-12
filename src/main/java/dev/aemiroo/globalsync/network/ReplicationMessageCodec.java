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
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/** Strict JSON message allowlist for the first replication protocol version. */
public final class ReplicationMessageCodec {
    private static final Set<String> EVENT_FIELDS = Set.of("type", "protocolVersion", "eventId",
            "worldId", "originServerId", "sequence", "createdAt", "eventType", "attributes");
    private static final Set<String> ACK_FIELDS = Set.of("type", "protocolVersion", "eventId", "journalOffset");
    private static final Set<String> BLOCK_ATTRIBUTES = Set.of("player_uuid", "x", "y", "z",
            "previous_block_data", "new_block_data");

    private ReplicationMessageCodec() { }

    public static byte[] encodeEvent(ReplicationEnvelope event) {
        JsonObject json = new JsonObject();
        json.addProperty("type", "EVENT");
        json.addProperty("protocolVersion", event.protocolVersion());
        json.addProperty("eventId", event.eventId().toString());
        json.addProperty("worldId", event.worldId());
        json.addProperty("originServerId", event.originServerId());
        json.addProperty("sequence", event.sequence());
        json.addProperty("createdAt", event.createdAt().toString());
        json.addProperty("eventType", event.eventType());
        JsonObject attrs = new JsonObject();
        new TreeMap<>(event.attributes()).forEach((key, value) -> attrs.addProperty(key, value));
        json.add("attributes", attrs);
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static ReplicationEnvelope decodeEvent(byte[] payload, String authenticatedPeerId,
                                                   Set<String> sharedWorlds) throws IOException {
        JsonObject json = parse(payload);
        requireFields(json, EVENT_FIELDS);
        if (!"EVENT".equals(string(json, "type")) || integer(json, "protocolVersion") != 1)
            throw new IOException("Unsupported replication message type or protocol version");
        String origin = string(json, "originServerId");
        if (!origin.equals(authenticatedPeerId))
            throw new IOException("Event origin does not match the authenticated peer");
        String world = string(json, "worldId");
        if (!sharedWorlds.contains(world))
            throw new IOException("Event references a world that is not shared with this node");
        String eventType = string(json, "eventType");
        if (!"BLOCK_PLACE".equals(eventType) && !"BLOCK_BREAK".equals(eventType))
            throw new IOException("Event type is not allowed by protocol version 1");
        JsonElement attrsElement = json.get("attributes");
        if (attrsElement == null || !attrsElement.isJsonObject())
            throw new IOException("Event attributes must be an object");
        JsonObject attrs = attrsElement.getAsJsonObject();
        requireFields(attrs, BLOCK_ATTRIBUTES);
        String player = string(attrs, "player_uuid");
        try {
            if (!UUID.fromString(player).toString().equalsIgnoreCase(player)) throw new IllegalArgumentException();
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid player UUID", exception);
        }
        int x = integerText(attrs, "x"), y = integerText(attrs, "y"), z = integerText(attrs, "z");
        String previous = boundedText(attrs, "previous_block_data", 512);
        String next = boundedText(attrs, "new_block_data", 512);
        if ("BLOCK_BREAK".equals(eventType) && !"minecraft:air".equals(next))
            throw new IOException("BLOCK_BREAK must result in minecraft:air");
        if ("BLOCK_PLACE".equals(eventType) && "minecraft:air".equals(next))
            throw new IOException("BLOCK_PLACE cannot result in minecraft:air");
        try {
            return new ReplicationEnvelope(1, UUID.fromString(string(json, "eventId")), world, origin,
                    longInteger(json, "sequence"), Instant.parse(string(json, "createdAt")), eventType,
                    Map.of("player_uuid", player, "x", Integer.toString(x), "y", Integer.toString(y),
                            "z", Integer.toString(z), "previous_block_data", previous, "new_block_data", next));
        } catch (RuntimeException exception) {
            throw new IOException("Invalid replication event envelope", exception);
        }
    }

    public static byte[] encodeAck(UUID eventId, long journalOffset) {
        JsonObject ack = new JsonObject();
        ack.addProperty("type", "ACK");
        ack.addProperty("protocolVersion", 1);
        ack.addProperty("eventId", eventId.toString());
        ack.addProperty("journalOffset", journalOffset);
        return ack.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static void validateAck(byte[] payload, UUID expectedEventId) throws IOException {
        JsonObject json = parse(payload);
        requireFields(json, ACK_FIELDS);
        if (!"ACK".equals(string(json, "type")) || integer(json, "protocolVersion") != 1
                || !expectedEventId.toString().equals(string(json, "eventId"))
                || longInteger(json, "journalOffset") < 1)
            throw new IOException("Invalid replication acknowledgement");
    }

    private static JsonObject parse(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length < 2 || bytes.length > FrameCodec.DEFAULT_MAX_FRAME_BYTES)
            throw new IOException("Invalid replication message size");
        final String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException exception) {
            throw new IOException("Replication message is not valid UTF-8", exception);
        }
        try {
            JsonElement parsed = JsonParser.parseString(text);
            if (!parsed.isJsonObject()) throw new IOException("Replication message must be a JSON object");
            return parsed.getAsJsonObject();
        } catch (RuntimeException exception) {
            throw new IOException("Malformed replication JSON", exception);
        }
    }
    private static void requireFields(JsonObject json, Set<String> expected) throws IOException {
        if (!json.keySet().equals(expected)) throw new IOException("Unexpected or missing replication message fields");
    }
    private static String string(JsonObject json, String key) throws IOException {
        JsonElement value = json.get(key);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString())
            throw new IOException("Invalid string field: " + key);
        String result = value.getAsString();
        if (result.isBlank() || result.length() > 256) throw new IOException("Invalid string field: " + key);
        return result;
    }
    private static String boundedText(JsonObject json, String key, int max) throws IOException {
        String value = string(json, key);
        if (value.length() > max) throw new IOException("Field exceeds its size limit: " + key);
        return value;
    }
    private static int integer(JsonObject json, String key) throws IOException {
        long value = longInteger(json, key);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) throw new IOException("Integer out of range: " + key);
        return (int) value;
    }
    private static int integerText(JsonObject json, String key) throws IOException {
        String raw = string(json, key);
        try {
            if (!raw.matches("-?(0|[1-9][0-9]*)")) throw new NumberFormatException();
            return Integer.parseInt(raw);
        } catch (NumberFormatException exception) {
            throw new IOException("Invalid coordinate: " + key, exception);
        }
    }
    private static long longInteger(JsonObject json, String key) throws IOException {
        JsonElement value = json.get(key);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber())
            throw new IOException("Invalid integer field: " + key);
        try {
            String raw = value.getAsString();
            if (!raw.matches("-?(0|[1-9][0-9]*)")) throw new NumberFormatException();
            return Long.parseLong(raw);
        } catch (NumberFormatException exception) {
            throw new IOException("Invalid integer field: " + key, exception);
        }
    }
}