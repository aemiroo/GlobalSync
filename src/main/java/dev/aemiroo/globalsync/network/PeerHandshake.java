package dev.aemiroo.globalsync.network;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

/** Small, strict, bidirectional identity handshake. It does not carry replication events. */
public final class PeerHandshake {
    public static final int PROTOCOL_VERSION = 1;
    private static final int MAX_HANDSHAKE_BYTES = 4_096;
    private static final Set<String> HELLO_FIELDS = Set.of("type", "protocolVersion", "serverId", "nonce");
    private static final Set<String> READY_FIELDS = Set.of("type", "nonce");

    private PeerHandshake() {
    }

    public static String authenticate(
            SSLSocket socket,
            String localServerId,
            PeerAuthorizationRegistry authorization,
            int maxFrameBytes
    ) throws IOException {
        if (socket == null || localServerId == null || localServerId.isBlank()
                || localServerId.length() > 64 || authorization == null) {
            throw new IllegalArgumentException("Invalid peer handshake arguments");
        }
        if (maxFrameBytes < MAX_HANDSHAKE_BYTES) {
            throw new IllegalArgumentException("maxFrameBytes must be at least " + MAX_HANDSHAKE_BYTES);
        }

        socket.startHandshake();
        String localNonce = UUID.randomUUID().toString();
        JsonObject hello = new JsonObject();
        hello.addProperty("type", "HELLO");
        hello.addProperty("protocolVersion", PROTOCOL_VERSION);
        hello.addProperty("serverId", localServerId);
        hello.addProperty("nonce", localNonce);
        write(socket, hello, maxFrameBytes);

        JsonObject remoteHello = read(socket, maxFrameBytes);
        requireExactFields(remoteHello, HELLO_FIELDS);
        requireCanonicalHello(remoteHello);
        if (!"HELLO".equals(string(remoteHello, "type"))
                || integer(remoteHello, "protocolVersion") != PROTOCOL_VERSION) {
            throw new IOException("Peer protocol handshake is incompatible");
        }

        String remoteServerId = string(remoteHello, "serverId");
        String remoteNonce = string(remoteHello, "nonce");
        try {
            if (!UUID.fromString(remoteNonce).toString().equalsIgnoreCase(remoteNonce)) {
                throw new IllegalArgumentException("Nonce is not in canonical UUID form");
            }
        } catch (IllegalArgumentException exception) {
            throw new IOException("Peer supplied an invalid handshake nonce", exception);
        }
        authorization.requireAuthorized(socket.getSession(), remoteServerId);

        JsonObject ready = new JsonObject();
        ready.addProperty("type", "READY");
        ready.addProperty("nonce", remoteNonce);
        write(socket, ready, maxFrameBytes);

        JsonObject remoteReady = read(socket, maxFrameBytes);
        requireExactFields(remoteReady, READY_FIELDS);
        requireCanonicalReady(remoteReady);
        if (!"READY".equals(string(remoteReady, "type"))
                || !localNonce.equals(string(remoteReady, "nonce"))) {
            throw new SSLPeerUnverifiedException("Peer did not confirm this connection's handshake nonce");
        }
        return remoteServerId;
    }

    private static void write(SSLSocket socket, JsonObject message, int maxFrameBytes) throws IOException {
        byte[] bytes = message.toString().getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_HANDSHAKE_BYTES) {
            throw new IOException("Peer handshake message is too large");
        }
        FrameCodec.writeFrame(socket.getOutputStream(), bytes, Math.min(maxFrameBytes, MAX_HANDSHAKE_BYTES));
    }

    private static JsonObject read(SSLSocket socket, int maxFrameBytes) throws IOException {
        byte[] bytes = FrameCodec.readFrame(socket.getInputStream(), Math.min(maxFrameBytes, MAX_HANDSHAKE_BYTES));
        try {
            JsonElement parsed = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8));
            if (!parsed.isJsonObject()) {
                throw new IOException("Peer handshake message must be a JSON object");
            }
            JsonObject object = parsed.getAsJsonObject();
            if (!object.toString().equals(new String(bytes, StandardCharsets.UTF_8))) {
                throw new IOException("Peer handshake JSON is not canonical");
            }
            return object;
        } catch (RuntimeException exception) {
            throw new IOException("Peer sent malformed handshake JSON", exception);
        }
    }

    private static void requireExactFields(JsonObject object, Set<String> expected) throws IOException {
        if (!object.keySet().equals(expected)) {
            throw new IOException("Peer handshake contains missing or unknown fields");
        }
    }

    private static void requireCanonicalHello(JsonObject object) throws IOException {
        JsonObject canonical = new JsonObject();
        canonical.addProperty("type", string(object, "type"));
        canonical.addProperty("protocolVersion", integer(object, "protocolVersion"));
        canonical.addProperty("serverId", string(object, "serverId"));
        canonical.addProperty("nonce", string(object, "nonce"));
        if (!canonical.equals(object)) {
            throw new IOException("Peer HELLO message is not canonical");
        }
    }

    private static void requireCanonicalReady(JsonObject object) throws IOException {
        JsonObject canonical = new JsonObject();
        canonical.addProperty("type", string(object, "type"));
        canonical.addProperty("nonce", string(object, "nonce"));
        if (!canonical.equals(object)) {
            throw new IOException("Peer READY message is not canonical");
        }
    }

    private static String string(JsonObject object, String field) throws IOException {
        JsonElement value = object.get(field);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new IOException("Peer handshake field has an invalid type: " + field);
        }
        String result = value.getAsString();
        if (result.isBlank() || result.length() > 128) {
            throw new IOException("Peer handshake field is outside the allowed length: " + field);
        }
        return result;
    }

    private static int integer(JsonObject object, String field) throws IOException {
        JsonElement value = object.get(field);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw new IOException("Peer handshake field has an invalid type: " + field);
        }
        try {
            java.math.BigDecimal number = value.getAsBigDecimal();
            return number.intValueExact();
        } catch (NumberFormatException exception) {
            throw new IOException("Peer handshake field is not an integer: " + field, exception);
        }
    }
}
