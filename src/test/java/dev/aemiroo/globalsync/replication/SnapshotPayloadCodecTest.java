package dev.aemiroo.globalsync.replication;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotPayloadCodecTest {
    private static final List<SnapshotPayloadCodec.BlockRecord> RECORDS = List.of(
            new SnapshotPayloadCodec.BlockRecord(-2, 60, 4, "minecraft:stone"),
            new SnapshotPayloadCodec.BlockRecord(-2, 60, 5, "minecraft:oak_log[axis=y]"),
            new SnapshotPayloadCodec.BlockRecord(0, -64, 0, "minecraft:deepslate"));

    @Test
    void writesAndReadsDeterministicPayload() throws Exception {
        byte[] first = encode(RECORDS, RECORDS.size());
        byte[] second = encode(RECORDS, RECORDS.size());
        assertArrayEquals(first, second);

        List<SnapshotPayloadCodec.BlockRecord> decoded = new ArrayList<>();
        SnapshotPayloadCodec.read(new ByteArrayInputStream(first), RECORDS.size(), decoded::add);
        assertEquals(RECORDS, decoded);
    }

    @Test
    void reportsExactLengthWithoutReiteratingInput() throws Exception {
        Iterable<SnapshotPayloadCodec.BlockRecord> oneShot = () -> RECORDS.iterator();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        long length = SnapshotPayloadCodec.write(output, oneShot, RECORDS.size());
        assertEquals(output.size(), length);
        assertEquals(output.size(), firstExpectedLength());
    }

    @Test
    void allowsEmptySnapshotPayload() throws Exception {
        byte[] payload = encode(List.of(), 0);
        assertEquals(SnapshotPayloadCodec.headerLength(), payload.length);
        assertDoesNotThrow(() -> SnapshotPayloadCodec.read(
                new ByteArrayInputStream(payload), 0, record -> fail("unexpected record")));
    }

    @Test
    void rejectsWrongMagicVersionAndRecordCount() throws Exception {
        byte[] valid = encode(RECORDS, RECORDS.size());
        byte[] badMagic = valid.clone();
        badMagic[0] = 0;
        assertThrows(IOException.class, () -> SnapshotPayloadCodec.read(
                new ByteArrayInputStream(badMagic), RECORDS.size(), record -> { }));

        byte[] badVersion = valid.clone();
        badVersion[4] = 2;
        assertThrows(IOException.class, () -> SnapshotPayloadCodec.read(
                new ByteArrayInputStream(badVersion), RECORDS.size(), record -> { }));

        assertThrows(IOException.class, () -> SnapshotPayloadCodec.read(
                new ByteArrayInputStream(valid), RECORDS.size() - 1, record -> { }));
    }

    @Test
    void rejectsUnsortedDuplicateAndMissingRecordsOnWrite() {
        List<SnapshotPayloadCodec.BlockRecord> unsorted = List.of(RECORDS.get(1), RECORDS.get(0));
        assertThrows(IOException.class, () -> encode(unsorted, unsorted.size()));
        List<SnapshotPayloadCodec.BlockRecord> duplicate = List.of(RECORDS.get(0), RECORDS.get(0));
        assertThrows(IOException.class, () -> encode(duplicate, duplicate.size()));
        assertThrows(IOException.class, () -> encode(RECORDS, RECORDS.size() + 1));
        assertThrows(IOException.class, () -> encode(RECORDS, RECORDS.size() - 1));
    }

    @Test
    void rejectsTruncatedRecordsTrailingBytesAndInvalidUtf8() throws Exception {
        byte[] valid = encode(RECORDS, RECORDS.size());
        assertThrows(IOException.class, () -> SnapshotPayloadCodec.read(
                new ByteArrayInputStream(java.util.Arrays.copyOf(valid, valid.length - 1)),
                RECORDS.size(), record -> { }));

        byte[] trailing = java.util.Arrays.copyOf(valid, valid.length + 1);
        assertThrows(IOException.class, () -> SnapshotPayloadCodec.read(
                new ByteArrayInputStream(trailing), RECORDS.size(), record -> { }));

        byte[] invalidUtf8 = encode(List.of(
                new SnapshotPayloadCodec.BlockRecord(0, 0, 0, "minecraft:stone")), 1);
        int dataOffset = 9 + 12 + 2 + "minecraft:".length();
        invalidUtf8[dataOffset] = (byte) 0xff;
        assertThrows(IOException.class, () -> SnapshotPayloadCodec.read(
                new ByteArrayInputStream(invalidUtf8), 1, record -> { }));
    }

    @Test
    void rejectsInvalidBlockData() {
        assertThrows(IllegalArgumentException.class,
                () -> new SnapshotPayloadCodec.BlockRecord(0, 0, 0, "stone"));
        assertThrows(IllegalArgumentException.class,
                () -> new SnapshotPayloadCodec.BlockRecord(0, 0, 0, "minecraft:stone;run"));
        assertThrows(IllegalArgumentException.class,
                () -> new SnapshotPayloadCodec.BlockRecord(0, 0, 0, "minecraft:stone[axis=y][x=z]"));
        assertThrows(IllegalArgumentException.class,
                () -> new SnapshotPayloadCodec.BlockRecord(0, 0, 0, ":stone"));
        assertThrows(IllegalArgumentException.class,
                () -> new SnapshotPayloadCodec.BlockRecord(0, 0, 0, "minecraft:mod:stone"));
    }

    @Test
    void rejectsPayloadRecordCountAboveHardLimit() {
        assertThrows(IOException.class, () -> encode(List.of(),
                SnapshotPayloadCodec.MAX_BLOCK_RECORDS + 1));
        assertThrows(IOException.class, () -> SnapshotPayloadCodec.read(
                new ByteArrayInputStream(new byte[0]),
                SnapshotPayloadCodec.MAX_BLOCK_RECORDS + 1, record -> { }));
    }

    private static byte[] encode(Iterable<SnapshotPayloadCodec.BlockRecord> records, long count)
            throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SnapshotPayloadCodec.write(output, records, count);
        return output.toByteArray();
    }

    private static long firstExpectedLength() {
        return SnapshotPayloadCodec.headerLength() + RECORDS.stream()
                .mapToLong(record -> 14L + record.blockData().getBytes(StandardCharsets.UTF_8).length)
                .sum();
    }
}
