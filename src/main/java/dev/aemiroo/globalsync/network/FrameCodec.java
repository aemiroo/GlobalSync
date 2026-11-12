package dev.aemiroo.globalsync.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * Length-prefixed binary framing for peer messages.
 *
 * The length is a signed, big-endian 32-bit integer. Empty and oversized
 * frames are rejected before allocating the payload buffer.
 */
public final class FrameCodec {
    public static final int DEFAULT_MAX_FRAME_BYTES = 1_048_576;

    private FrameCodec() {
    }

    public static byte[] readFrame(InputStream input) throws IOException {
        return readFrame(input, DEFAULT_MAX_FRAME_BYTES);
    }

    public static byte[] readFrame(InputStream input, int maxFrameBytes) throws IOException {
        Objects.requireNonNull(input, "input");
        validateLimit(maxFrameBytes);

        DataInputStream data = new DataInputStream(input);
        final int length;
        try {
            length = data.readInt();
        } catch (EOFException exception) {
            throw exception;
        }

        if (length < 1 || length > maxFrameBytes) {
            throw new IOException("Peer frame length is outside the allowed range");
        }

        byte[] payload = data.readNBytes(length);
        if (payload.length != length) {
            throw new EOFException("Peer closed the connection during a frame");
        }
        return payload;
    }

    public static void writeFrame(OutputStream output, byte[] payload) throws IOException {
        writeFrame(output, payload, DEFAULT_MAX_FRAME_BYTES);
    }

    public static void writeFrame(OutputStream output, byte[] payload, int maxFrameBytes) throws IOException {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(payload, "payload");
        validateLimit(maxFrameBytes);
        if (payload.length < 1 || payload.length > maxFrameBytes) {
            throw new IOException("Peer frame length is outside the allowed range");
        }

        DataOutputStream data = new DataOutputStream(output);
        data.writeInt(payload.length);
        data.write(payload);
        data.flush();
    }

    private static void validateLimit(int maxFrameBytes) {
        if (maxFrameBytes < 1) {
            throw new IllegalArgumentException("maxFrameBytes must be positive");
        }
    }
}
