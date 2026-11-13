package dev.aemiroo.globalsync.network;

import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class PeerControlMessageTest {
    @Test
    void roundTripsPingAndPongWithNonce() throws Exception {
        UUID nonce = UUID.randomUUID();
        assertTrue(PeerControlMessage.isControl(PeerControlMessage.encodePing(nonce)));
        assertEquals(new PeerControlMessage.Message(PeerControlMessage.Type.PING, nonce),
                PeerControlMessage.decode(PeerControlMessage.encodePing(nonce)));
        assertEquals(new PeerControlMessage.Message(PeerControlMessage.Type.PONG, nonce),
                PeerControlMessage.decode(PeerControlMessage.encodePong(nonce)));
    }

    @Test
    void rejectsUnknownFieldsUnsupportedVersionsAndMalformedFrames() {
        assertThrows(java.io.IOException.class, () -> PeerControlMessage.decode(
                ("{\"type\":\"PING\",\"protocolVersion\":2,\"nonce\":\""
                        + UUID.randomUUID() + "\",\"extra\":true}").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertThrows(java.io.IOException.class, () -> PeerControlMessage.decode(
                ("{\"type\":\"PING\",\"protocolVersion\":1,\"nonce\":\""
                        + UUID.randomUUID() + "\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertThrows(java.io.IOException.class, () -> PeerControlMessage.decode(new byte[]{'{','}'}));
    }
}
