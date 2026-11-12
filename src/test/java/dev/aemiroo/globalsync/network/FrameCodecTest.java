package dev.aemiroo.globalsync.network;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class FrameCodecTest {
    @Test
    void roundTripsPayload() throws IOException {
        byte[] payload = new byte[]{1, 2, 3, 4};
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        FrameCodec.writeFrame(output, payload);
        assertArrayEquals(payload, FrameCodec.readFrame(new ByteArrayInputStream(output.toByteArray())));
    }

    @Test
    void rejectsEmptyFrame() {
        assertThrows(IOException.class, () ->
                FrameCodec.writeFrame(new ByteArrayOutputStream(), new byte[0]));
    }

    @Test
    void rejectsOversizedFrameBeforeReadingPayload() {
        byte[] header = new byte[]{0, 0, 0, 9};
        assertThrows(IOException.class, () ->
                FrameCodec.readFrame(new ByteArrayInputStream(header), 8));
    }

    @Test
    void rejectsTruncatedPayload() {
        byte[] truncated = new byte[]{0, 0, 0, 3, 1, 2};
        assertThrows(EOFException.class, () ->
                FrameCodec.readFrame(new ByteArrayInputStream(truncated)));
    }

    @Test
    void rejectsNonPositiveFrameLimit() {
        assertThrows(IllegalArgumentException.class, () ->
                FrameCodec.readFrame(new ByteArrayInputStream(new byte[0]), 0));
    }
}
