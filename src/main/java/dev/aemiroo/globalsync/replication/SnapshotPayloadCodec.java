package dev.aemiroo.globalsync.replication;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;

/**
 * Deterministic, streaming binary representations of PAPER_BLOCK_DATA_V1 and V2.
 *
 * <p>The payload contains only explicit chunk coverage, block coordinates, and canonical Bukkit
 * BlockData strings. It is not a world archive and has no path, entity,
 * inventory, block-entity, or executable content.</p>
 */
public final class SnapshotPayloadCodec {
    private static final int MAGIC = 0x47534231; // "GSB1"
    private static final int VERSION = 1;
    private static final int COVERAGE_VERSION = 2;
    private static final int COVERAGE_MAGIC = 0x47534232; // "GSB2"
    private static final int HEADER_BYTES = 9;
    public static final int MAX_BLOCK_DATA_BYTES = 1024;
    public static final long MAX_BLOCK_RECORDS = 16_777_216L;

    public record BlockRecord(int x, int y, int z, String blockData) {
        public BlockRecord {
            Objects.requireNonNull(blockData, "blockData");
            byte[] encoded = blockData.getBytes(StandardCharsets.UTF_8);
            if (encoded.length < 1 || encoded.length > MAX_BLOCK_DATA_BYTES
                    || !blockData.equals(decodeUtf8(encoded))
                    || !isValidBlockData(blockData)) {
                throw new IllegalArgumentException("Invalid snapshot block data");
            }
        }
    }

    /** A chunk whose complete block-state contents are represented by a V2 payload. */
    public record ChunkCoordinate(int x, int z) implements Comparable<ChunkCoordinate> {
        @Override public int compareTo(ChunkCoordinate other) {
            int byX = Integer.compare(x, other.x);
            return byX != 0 ? byX : Integer.compare(z, other.z);
        }
    }

    @FunctionalInterface
    public interface BlockRecordConsumer {
        void accept(BlockRecord record) throws IOException;
    }

    @FunctionalInterface
    public interface CoveredChunkConsumer {
        void accept(ChunkCoordinate chunk) throws IOException;
    }

    private SnapshotPayloadCodec() { }

    /**
     * Writes exactly expectedRecords in strict coordinate order. The caller
     * owns the output stream; this method does not close it.
     */
    public static long write(OutputStream output, Iterable<BlockRecord> records,
                             long expectedRecords) throws IOException {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(records, "records");
        if (expectedRecords < 0 || expectedRecords > MAX_BLOCK_RECORDS)
            throw new IOException("Snapshot block count exceeds the supported limit");

        DataOutputStream data = new DataOutputStream(output);
        data.writeInt(MAGIC);
        data.writeByte(VERSION);
        data.writeInt((int) expectedRecords);

        long written = 0;
        long totalLength = HEADER_BYTES;
        BlockRecord previous = null;
        for (BlockRecord record : records) {
            if (record == null) throw new IOException("Snapshot contains a null block record");
            if (written >= expectedRecords)
                throw new IOException("Snapshot contains more records than declared");
            if (previous != null && compare(previous, record) >= 0)
                throw new IOException("Snapshot block records are not strictly coordinate-sorted");

            byte[] blockData = record.blockData().getBytes(StandardCharsets.UTF_8);
            if (blockData.length < 1 || blockData.length > MAX_BLOCK_DATA_BYTES)
                throw new IOException("Snapshot block data exceeds its size limit");
            data.writeInt(record.x());
            data.writeInt(record.y());
            data.writeInt(record.z());
            data.writeShort(blockData.length);
            data.write(blockData);
            totalLength = Math.addExact(totalLength, 14L + blockData.length);
            previous = record;
            written++;
        }
        if (written != expectedRecords)
            throw new IOException("Snapshot contains fewer records than declared");
        data.flush();
        return totalLength;
    }

    /**
     * Writes PAPER_BLOCK_DATA_V2, which records the exact chunk coverage before
     * the block records. A receiver can therefore clear covered chunks before
     * applying the listed non-air states without touching chunks outside the
     * captured set. Inputs are streamed; only the bounded coverage index is held.
     */
    public static long writeV2(OutputStream output, Iterable<ChunkCoordinate> coveredChunks,
                               long expectedChunks, Iterable<BlockRecord> records,
                               long expectedRecords) throws IOException {
        Objects.requireNonNull(output, "output");
        Objects.requireNonNull(coveredChunks, "coveredChunks");
        Objects.requireNonNull(records, "records");
        if (expectedChunks < 0 || expectedChunks > 65_536L)
            throw new IOException("Snapshot covered-chunk count exceeds the supported limit");
        if (expectedRecords < 0 || expectedRecords > MAX_BLOCK_RECORDS)
            throw new IOException("Snapshot block count exceeds the supported limit");

        DataOutputStream data = new DataOutputStream(output);
        data.writeInt(COVERAGE_MAGIC);
        data.writeByte(COVERAGE_VERSION);
        data.writeInt((int) expectedChunks);
        Set<ChunkCoordinate> coverage = new HashSet<>((int) Math.min(expectedChunks * 2 + 1, 131_073));
        ChunkCoordinate previousChunk = null;
        long writtenChunks = 0;
        long totalLength = 9L;
        for (ChunkCoordinate chunk : coveredChunks) {
            if (chunk == null || writtenChunks >= expectedChunks
                    || (previousChunk != null && previousChunk.compareTo(chunk) >= 0))
                throw new IOException("Snapshot covered chunks are not strictly sorted or exceed their declared count");
            coverage.add(chunk);
            data.writeInt(chunk.x());
            data.writeInt(chunk.z());
            totalLength = Math.addExact(totalLength, 8L);
            previousChunk = chunk;
            writtenChunks++;
        }
        if (writtenChunks != expectedChunks)
            throw new IOException("Snapshot contains fewer covered chunks than declared");

        data.writeInt((int) expectedRecords);
        totalLength = Math.addExact(totalLength, 4L);
        BlockRecord previous = null;
        long writtenRecords = 0;
        for (BlockRecord record : records) {
            if (record == null || writtenRecords >= expectedRecords
                    || (previous != null && compare(previous, record) >= 0))
                throw new IOException("Snapshot block records are not strictly sorted or exceed their declared count");
            if (!coverage.contains(new ChunkCoordinate(record.x() >> 4, record.z() >> 4)))
                throw new IOException("Snapshot block record lies outside declared chunk coverage");
            byte[] blockData = record.blockData().getBytes(StandardCharsets.UTF_8);
            if (blockData.length < 1 || blockData.length > MAX_BLOCK_DATA_BYTES)
                throw new IOException("Snapshot block data exceeds its size limit");
            data.writeInt(record.x());
            data.writeInt(record.y());
            data.writeInt(record.z());
            data.writeShort(blockData.length);
            data.write(blockData);
            totalLength = Math.addExact(totalLength, 14L + blockData.length);
            previous = record;
            writtenRecords++;
        }
        if (writtenRecords != expectedRecords)
            throw new IOException("Snapshot contains fewer records than declared");
        data.flush();
        return totalLength;
    }

    /**
     * Reads and validates one payload, invoking the consumer per record.
     * The stream is not closed. Trailing bytes are rejected.
     */
    public static void read(InputStream input, long expectedRecords,
                            BlockRecordConsumer consumer) throws IOException {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(consumer, "consumer");
        if (expectedRecords < 0 || expectedRecords > MAX_BLOCK_RECORDS)
            throw new IOException("Snapshot block count exceeds the supported limit");

        DataInputStream data = new DataInputStream(new BufferedInputStream(input));
        if (data.readInt() != MAGIC) throw new IOException("Invalid snapshot payload magic");
        if (data.readUnsignedByte() != VERSION)
            throw new IOException("Unsupported snapshot payload version");
        long count = Integer.toUnsignedLong(data.readInt());
        if (count != expectedRecords || count > MAX_BLOCK_RECORDS)
            throw new IOException("Snapshot payload record count does not match its manifest");

        BlockRecord previous = null;
        for (long index = 0; index < count; index++) {
            final int x, y, z, length;
            try {
                x = data.readInt();
                y = data.readInt();
                z = data.readInt();
                length = data.readUnsignedShort();
            } catch (EOFException exception) {
                throw new IOException("Snapshot payload ended inside a block record", exception);
            }
            if (length < 1 || length > MAX_BLOCK_DATA_BYTES)
                throw new IOException("Snapshot block data length is invalid");
            byte[] bytes = data.readNBytes(length);
            if (bytes.length != length) throw new IOException("Snapshot block data is truncated");
            String blockData = decodeStrict(bytes);
            final BlockRecord record;
            try {
                record = new BlockRecord(x, y, z, blockData);
            } catch (IllegalArgumentException exception) {
                throw new IOException("Snapshot contains invalid block data", exception);
            }
            if (previous != null && compare(previous, record) >= 0)
                throw new IOException("Snapshot block records are not strictly coordinate-sorted");
            consumer.accept(record);
            previous = record;
        }
        if (data.read() != -1) throw new IOException("Snapshot payload contains trailing bytes");
    }

    /** Reads and validates a V2 payload, reporting its explicit chunk coverage. */
    public static long readV2(InputStream input, long expectedRecords,
                              CoveredChunkConsumer chunkConsumer,
                              BlockRecordConsumer recordConsumer) throws IOException {
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(chunkConsumer, "chunkConsumer");
        Objects.requireNonNull(recordConsumer, "recordConsumer");
        if (expectedRecords < 0 || expectedRecords > MAX_BLOCK_RECORDS)
            throw new IOException("Snapshot block count exceeds the supported limit");
        DataInputStream data = new DataInputStream(new BufferedInputStream(input));
        if (data.readInt() != COVERAGE_MAGIC || data.readUnsignedByte() != COVERAGE_VERSION)
            throw new IOException("Invalid snapshot coverage payload header");
        long chunkCount = Integer.toUnsignedLong(data.readInt());
        if (chunkCount > 65_536L) throw new IOException("Snapshot covered-chunk count exceeds the supported limit");
        Set<ChunkCoordinate> coverage = new HashSet<>((int) Math.min(chunkCount * 2 + 1, 131_073));
        ChunkCoordinate previousChunk = null;
        for (long index = 0; index < chunkCount; index++) {
            final ChunkCoordinate chunk;
            try {
                chunk = new ChunkCoordinate(data.readInt(), data.readInt());
            } catch (EOFException exception) {
                throw new IOException("Snapshot payload ended inside its chunk coverage header", exception);
            }
            if (previousChunk != null && previousChunk.compareTo(chunk) >= 0)
                throw new IOException("Snapshot covered chunks are not strictly sorted");
            chunkConsumer.accept(chunk);
            coverage.add(chunk);
            previousChunk = chunk;
        }
        long count = Integer.toUnsignedLong(data.readInt());
        if (count != expectedRecords || count > MAX_BLOCK_RECORDS)
            throw new IOException("Snapshot payload record count does not match its manifest");
        BlockRecord previous = null;
        for (long index = 0; index < count; index++) {
            final int x, y, z, length;
            try {
                x = data.readInt(); y = data.readInt(); z = data.readInt(); length = data.readUnsignedShort();
            } catch (EOFException exception) {
                throw new IOException("Snapshot payload ended inside a block record", exception);
            }
            if (length < 1 || length > MAX_BLOCK_DATA_BYTES)
                throw new IOException("Snapshot block data length is invalid");
            byte[] bytes = data.readNBytes(length);
            if (bytes.length != length) throw new IOException("Snapshot block data is truncated");
            final BlockRecord record;
            try {
                record = new BlockRecord(x, y, z, decodeStrict(bytes));
            } catch (IllegalArgumentException exception) {
                throw new IOException("Snapshot contains invalid block data", exception);
            }
            if (previous != null && compare(previous, record) >= 0)
                throw new IOException("Snapshot block records are not strictly coordinate-sorted");
            if (!coverage.contains(new ChunkCoordinate(x >> 4, z >> 4)))
                throw new IOException("Snapshot block record lies outside declared chunk coverage");
            recordConsumer.accept(record);
            previous = record;
        }
        if (data.read() != -1) throw new IOException("Snapshot payload contains trailing bytes");
        return chunkCount;
    }

    public static long headerLength() {
        return HEADER_BYTES;
    }

    private static int compare(BlockRecord left, BlockRecord right) {
        int x = Integer.compare(left.x(), right.x());
        if (x != 0) return x;
        int y = Integer.compare(left.y(), right.y());
        return y != 0 ? y : Integer.compare(left.z(), right.z());
    }

    private static boolean isValidBlockData(String value) {
        int properties = value.indexOf('[');
        String material = properties < 0 ? value : value.substring(0, properties);
        int separator = material.indexOf(':');
        if (separator < 1 || separator == material.length() - 1
                || material.indexOf(':', separator + 1) >= 0) return false;
        return value.matches("[a-z0-9_.-]+:[a-z0-9_./-]+(?:\\[[a-z0-9_./,=-]+\\])?");
    }

    private static String decodeStrict(byte[] bytes) throws IOException {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException exception) {
            throw new IOException("Snapshot block data is not valid UTF-8", exception);
        }
    }

    private static String decodeUtf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
