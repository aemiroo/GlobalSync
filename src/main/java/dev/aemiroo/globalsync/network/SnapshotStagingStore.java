package dev.aemiroo.globalsync.network;

import dev.aemiroo.globalsync.replication.SnapshotManifest;
import dev.aemiroo.globalsync.replication.SnapshotManifestCodec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Durable, isolated receiver-side snapshot staging. Files are named only from
 * validated snapshot UUIDs; no peer-supplied path is ever accepted.
 *
 * <p>Chunk data is forced to disk before its SQLite progress record is
 * committed. On recovery, bytes beyond the committed prefix are truncated.
 * A snapshot is COMPLETE only after exact length and SHA-256 verification.</p>
 */
public final class SnapshotStagingStore implements AutoCloseable {
    public static final long MAX_SNAPSHOT_BYTES = 512L * 1024 * 1024;
    public static final long MAX_RESERVED_BYTES = 1024L * 1024 * 1024;
    public static final int MAX_ACTIVE_TRANSFERS = 4;

    public record BeginResult(long nextChunk, boolean alreadyComplete) { }
    public record AppendResult(long nextChunk, boolean duplicate) { }

    private final Path root;
    private final Path dataDirectory;
    private final Connection connection;

    public SnapshotStagingStore(Path directory) throws IOException, SQLException {
        Objects.requireNonNull(directory, "directory");
        Path requested = directory.toAbsolutePath().normalize();
        Files.createDirectories(requested);
        if (Files.isSymbolicLink(requested) || !Files.isDirectory(requested, LinkOption.NOFOLLOW_LINKS))
            throw new IOException("Snapshot staging root must be a real directory, not a symbolic link");
        this.root = requested.toRealPath(LinkOption.NOFOLLOW_LINKS);
        this.dataDirectory = root.resolve("data");
        Files.createDirectories(dataDirectory);
        if (Files.isSymbolicLink(dataDirectory)
                || !Files.isDirectory(dataDirectory, LinkOption.NOFOLLOW_LINKS)
                || !dataDirectory.toRealPath(LinkOption.NOFOLLOW_LINKS).startsWith(root))
            throw new IOException("Snapshot staging data directory is unsafe");

        Path database = root.resolve("staging.db");
        if (Files.isSymbolicLink(database))
            throw new IOException("Snapshot staging database must not be a symbolic link");
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA synchronous=FULL");
            statement.execute("PRAGMA busy_timeout=5000");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS staged_snapshots (
                        snapshot_id TEXT PRIMARY KEY,
                        peer_id TEXT NOT NULL,
                        request_id TEXT NOT NULL,
                        manifest BLOB NOT NULL,
                        content_length INTEGER NOT NULL,
                        received_bytes INTEGER NOT NULL,
                        next_chunk INTEGER NOT NULL,
                        state TEXT NOT NULL CHECK(state IN ('RECEIVING','COMPLETE'))
                    )
                    """);
        }
        recoverIncompleteFiles();
    }

    public synchronized BeginResult begin(String authenticatedPeerId, UUID requestId,
                                          SnapshotManifest manifest) throws IOException {
        requirePeer(authenticatedPeerId);
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(manifest, "manifest");
        if (!authenticatedPeerId.equals(manifest.sourceServerId()))
            throw new IOException("Snapshot source does not match authenticated peer");
        if (manifest.contentLengthBytes() > MAX_SNAPSHOT_BYTES)
            throw new IOException("Snapshot exceeds the local staging size policy");

        byte[] manifestBytes = SnapshotManifestCodec.encode(manifest);
        try {
            Row existing = find(manifest.snapshotId());
            if (existing != null) {
                if (!existing.peerId.equals(authenticatedPeerId)
                        || !existing.requestId.equals(requestId.toString())
                        || !MessageDigest.isEqual(existing.manifest, manifestBytes))
                    throw new IOException("Snapshot ID is already bound to another transfer");
                Path file = dataPath(manifest.snapshotId());
                if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)
                        || Files.isSymbolicLink(file)
                        || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS))
                    throw new IOException("Staged snapshot file is missing or unsafe");
                try (FileChannel channel = openFile(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    if (channel.size() < existing.receivedBytes)
                        throw new IOException("Staged snapshot is shorter than its durable progress record");
                    if (channel.size() > existing.receivedBytes) {
                        channel.truncate(existing.receivedBytes);
                        channel.force(true);
                    }
                }
                return new BeginResult(existing.nextChunk, "COMPLETE".equals(existing.state));
            }

            if (countActive() >= MAX_ACTIVE_TRANSFERS)
                throw new IOException("Snapshot staging concurrency limit reached");
            if (reservedBytes() + manifest.contentLengthBytes() > MAX_RESERVED_BYTES)
                throw new IOException("Snapshot staging disk reservation limit reached");
            if (hasActivePeer(authenticatedPeerId))
                throw new IOException("Peer already has an active snapshot transfer");

            Path file = dataPath(manifest.snapshotId());
            try (FileChannel ignored = openFile(file, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                // CREATE_NEW and NOFOLLOW_LINKS prevent replacement through a symlink.
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO staged_snapshots(snapshot_id,peer_id,request_id,manifest,
                      content_length,received_bytes,next_chunk,state)
                    VALUES(?,?,?,?,?,0,0,'RECEIVING')
                    """)) {
                insert.setString(1, manifest.snapshotId());
                insert.setString(2, authenticatedPeerId);
                insert.setString(3, requestId.toString());
                insert.setBytes(4, manifestBytes);
                insert.setLong(5, manifest.contentLengthBytes());
                insert.executeUpdate();
            } catch (SQLException exception) {
                Files.deleteIfExists(file);
                throw exception;
            }
            return new BeginResult(0, false);
        } catch (SQLException exception) {
            throw new IOException("Could not persist snapshot staging state", exception);
        }
    }

    public synchronized AppendResult append(String authenticatedPeerId, UUID requestId,
            UUID snapshotId, long chunkIndex, byte[] bytes) throws IOException {
        requirePeer(authenticatedPeerId);
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(snapshotId, "snapshotId");
        if (bytes == null || bytes.length < 1 || bytes.length > SnapshotTransferMessage.CHUNK_BYTES)
            throw new IOException("Snapshot chunk size is outside the allowed range");
        try {
            Row row = requireRow(authenticatedPeerId, requestId, snapshotId);
            if (!"RECEIVING".equals(row.state))
                throw new IOException("Snapshot transfer is not receiving data");
            SnapshotManifest manifest = SnapshotManifestCodec.decode(row.manifest);
            SnapshotTransferMessage.Message chunk = new SnapshotTransferMessage.Message(
                    SnapshotTransferMessage.Type.CHUNK, requestId, snapshotId, null, chunkIndex, bytes, null);
            SnapshotTransferMessage.validateChunkForManifest(chunk, manifest);
            long offset = Math.multiplyExact(chunkIndex, (long) SnapshotTransferMessage.CHUNK_BYTES);
            Path file = dataPath(snapshotId.toString());

            if (chunkIndex < row.nextChunk) {
                if (!sameBytes(file, offset, bytes))
                    throw new IOException("Conflicting duplicate snapshot chunk");
                return new AppendResult(row.nextChunk, true);
            }
            if (chunkIndex != row.nextChunk || offset != row.receivedBytes)
                throw new IOException("Snapshot chunks must arrive in strict sequence");

            try (FileChannel channel = openFile(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                channel.position(offset);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }

            try (PreparedStatement update = connection.prepareStatement("""
                    UPDATE staged_snapshots
                    SET received_bytes=?, next_chunk=?
                    WHERE snapshot_id=? AND peer_id=? AND request_id=?
                      AND state='RECEIVING' AND next_chunk=? AND received_bytes=?
                    """)) {
                update.setLong(1, row.receivedBytes + bytes.length);
                update.setLong(2, row.nextChunk + 1);
                update.setString(3, snapshotId.toString());
                update.setString(4, authenticatedPeerId);
                update.setString(5, requestId.toString());
                update.setLong(6, row.nextChunk);
                update.setLong(7, row.receivedBytes);
                if (update.executeUpdate() != 1)
                    throw new IOException("Snapshot progress changed concurrently");
            }
            return new AppendResult(row.nextChunk + 1, false);
        } catch (SQLException | ArithmeticException exception) {
            throw new IOException("Could not durably stage snapshot chunk", exception);
        }
    }

    public synchronized void finish(String authenticatedPeerId, UUID requestId,
                                    UUID snapshotId) throws IOException {
        requirePeer(authenticatedPeerId);
        try {
            Row row = requireRow(authenticatedPeerId, requestId, snapshotId);
            if ("COMPLETE".equals(row.state)) return;
            SnapshotManifest manifest = SnapshotManifestCodec.decode(row.manifest);
            long expectedChunks = (manifest.contentLengthBytes()
                    + SnapshotTransferMessage.CHUNK_BYTES - 1) / SnapshotTransferMessage.CHUNK_BYTES;
            if (row.receivedBytes != manifest.contentLengthBytes() || row.nextChunk != expectedChunks)
                throw new IOException("Snapshot transfer ended before all manifest bytes arrived");

            Path file = dataPath(snapshotId.toString());
            if (Files.isSymbolicLink(file) || Files.size(file) != manifest.contentLengthBytes())
                throw new IOException("Staged snapshot length does not match its manifest");
            String digest = sha256(file);
            byte[] actual = HexFormat.of().parseHex(digest);
            byte[] expected = HexFormat.of().parseHex(manifest.contentSha256());
            if (!MessageDigest.isEqual(actual, expected))
                throw new IOException("Staged snapshot SHA-256 does not match its manifest");

            try (PreparedStatement update = connection.prepareStatement("""
                    UPDATE staged_snapshots SET state='COMPLETE'
                    WHERE snapshot_id=? AND peer_id=? AND request_id=? AND state='RECEIVING'
                    """)) {
                update.setString(1, snapshotId.toString());
                update.setString(2, authenticatedPeerId);
                update.setString(3, requestId.toString());
                if (update.executeUpdate() != 1)
                    throw new IOException("Snapshot completion state changed concurrently");
            }
        } catch (SQLException exception) {
            throw new IOException("Could not persist snapshot completion", exception);
        }
    }

    public synchronized boolean isComplete(UUID snapshotId) throws IOException {
        try {
            Row row = find(snapshotId.toString());
            return row != null && "COMPLETE".equals(row.state);
        } catch (SQLException exception) {
            throw new IOException("Could not inspect snapshot staging state", exception);
        }
    }

    private void recoverIncompleteFiles() throws IOException, SQLException {
        try (Statement query = connection.createStatement();
             ResultSet rows = query.executeQuery("SELECT snapshot_id,received_bytes,state FROM staged_snapshots")) {
            while (rows.next()) {
                String id = rows.getString(1);
                Path file = dataPath(id);
                if (Files.isSymbolicLink(file) || !Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS))
                    throw new IOException("A staged snapshot file is missing or unsafe");
                long committed = rows.getLong(2);
                try (FileChannel channel = openFile(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                    if (channel.size() < committed)
                        throw new IOException("A staged snapshot is shorter than its durable progress record");
                    if (channel.size() > committed && "RECEIVING".equals(rows.getString(3))) {
                        channel.truncate(committed);
                        channel.force(true);
                    }
                }
            }
        }
    }

    private Row requireRow(String peerId, UUID requestId, UUID snapshotId) throws SQLException, IOException {
        Row row = find(snapshotId.toString());
        if (row == null || !row.peerId.equals(peerId) || !row.requestId.equals(requestId.toString()))
            throw new IOException("Snapshot message does not match an active authenticated transfer");
        return row;
    }

    private Row find(String snapshotId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("""
                SELECT peer_id,request_id,manifest,received_bytes,next_chunk,state
                FROM staged_snapshots WHERE snapshot_id=?
                """)) {
            query.setString(1, snapshotId);
            try (ResultSet result = query.executeQuery()) {
                if (!result.next()) return null;
                return new Row(result.getString(1), result.getString(2), result.getBytes(3),
                        result.getLong(4), result.getLong(5), result.getString(6));
            }
        }
    }

    private long countActive() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery("SELECT COUNT(*) FROM staged_snapshots WHERE state='RECEIVING'")) {
            return result.next() ? result.getLong(1) : 0;
        }
    }

    private long reservedBytes() throws SQLException {
        try (Statement query = connection.createStatement();
             ResultSet result = query.executeQuery("SELECT COALESCE(SUM(content_length),0) FROM staged_snapshots")) {
            return result.next() ? result.getLong(1) : 0;
        }
    }

    private boolean hasActivePeer(String peerId) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(
                "SELECT 1 FROM staged_snapshots WHERE peer_id=? AND state='RECEIVING' LIMIT 1")) {
            query.setString(1, peerId);
            try (ResultSet result = query.executeQuery()) { return result.next(); }
        }
    }

    private boolean sameBytes(Path file, long offset, byte[] expected) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(expected.length);
        try (FileChannel channel = openFile(file, StandardOpenOption.READ)) {
            channel.position(offset);
            while (buffer.hasRemaining()) {
                if (channel.read(buffer) < 0) return false;
            }
        }
        return MessageDigest.isEqual(buffer.array(), expected);
    }

    private Path dataPath(String snapshotId) throws IOException {
        final UUID id;
        try {
            id = UUID.fromString(snapshotId);
            if (!id.toString().equals(snapshotId)) throw new IllegalArgumentException();
        } catch (IllegalArgumentException exception) {
            throw new IOException("Invalid snapshot identifier", exception);
        }
        Path file = dataDirectory.resolve(id + ".part").normalize();
        if (!file.getParent().equals(dataDirectory) || Files.isSymbolicLink(file))
            throw new IOException("Snapshot staging path is unsafe");
        return file;
    }

    private static FileChannel openFile(Path file, OpenOption... options) throws IOException {
        Set<OpenOption> safe = new java.util.HashSet<>(Set.of(options));
        safe.add(LinkOption.NOFOLLOW_LINKS);
        return FileChannel.open(file, safe);
    }

    private static String sha256(Path file) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = input.read(buffer)) != -1) digest.update(buffer, 0, read);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static void requirePeer(String peerId) throws IOException {
        if (peerId == null || !peerId.matches("[A-Za-z0-9._-]{1,64}"))
            throw new IOException("Invalid authenticated peer identity");
    }

    @Override
    public synchronized void close() throws SQLException {
        connection.close();
    }

    private record Row(String peerId, String requestId, byte[] manifest,
                       long receivedBytes, long nextChunk, String state) { }
}
