# Snapshot format, transfer, and consistency contract

This document defines the metadata and transfer contract for world-state snapshots. The manifest, strict protocol-v2 codec, receiver-side durable staging, and stop-and-wait outbound sender are implemented. Snapshot capture/production and live-world installation are not implemented; the sender accepts a completed local payload and its manifest from a caller.

## Scope

The first snapshot data format is `PAPER_BLOCK_DATA_V1`, scoped to one configured shared world and the currently supported block-state replication domain. It is not a complete Minecraft world backup. Player inventories, entities, block entities and container contents, scheduled ticks, fluids, redstone, weather, time, gamerules, and other environmental simulation are outside this format.

No snapshot payload writer exists yet. The exact record framing and capture implementation must be specified and tested before a node emits a manifest for an actual snapshot. The manifest codec must not be interpreted as proof that a snapshot file exists or is usable.

## Manifest

A version-1 manifest contains:

- `snapshotId`: canonical UUID identifying one immutable snapshot.
- `worldId`: one shared-world identifier.
- `sourceServerId`: the node that captured the snapshot.
- `createdAt`: canonical UTC-capable ISO-8601 instant; diagnostic metadata only.
- `dataFormat`: currently `PAPER_BLOCK_DATA_V1`.
- `contentLengthBytes`: exact payload byte length, from 1 byte through 64 GiB.
- `contentSha256`: lowercase SHA-256 hex digest of the exact payload bytes.
- `blockCount`: number of block-state records in the payload; zero is valid.
- `appliedThrough`: a bounded vector mapping each origin server ID to the highest contiguous origin sequence whose effects are represented in the snapshot.

The source node must appear in `appliedThrough`. A sequence value of zero means no event from that origin is included. Values are origin-local; they are not journal offsets and must never be compared between different origins. The vector is limited to 64 origins. Manifest JSON is canonical, rejects unknown or missing fields, and is limited to 64 KiB.

## Watermark meaning

`appliedThrough[origin]` is not the highest sequence merely received or observed. It is the highest contiguous sequence from that origin whose effects are reflected in the captured world state.

A future capture implementation must not advance an origin watermark across a missing sequence or an event whose winning state is still pending application. A superseded event may be covered when the later winning state that replaces it is also represented in the snapshot. A receiver-side durable ACK alone is insufficient evidence: ACK currently means inbox acceptance, not world application.

The vector is a per-origin coverage boundary, not a global commit index. It is useful for deciding which origin events need replay after a snapshot, but it does not create a total order across independent origins or prove causal closure for state outside the current block-edit contract.

## Capture boundary and event overlap

A snapshot must represent one coherent world-state cut. Copying live world files while the server continues mutating them is not an acceptable capture method.

The future Paper capture service must establish a server-thread boundary and coordinate with block application so a block mutation cannot occur halfway through the captured state. It must also account for events accepted while a potentially long capture is in progress. Those events must either be excluded from the snapshot and replayed after its vector boundary, or be included in both the captured state and its corresponding watermark; they must never be silently omitted from both.

A future implementation must define bounded capture work and interruption behavior. It must not force-load chunks or perform Bukkit world access from a worker thread. The capture mechanism must explicitly address the fact that the current plugin only journals successful player block placements and breaks; it does not capture environmental simulation or every world mutation.

## Validation and installation rules

Before a future receiver installs a snapshot, it must validate the manifest, exact byte length, SHA-256 digest, supported format, configured world identity, source peer identity, and all origin-vector entries. It must stage data separately and must not overwrite a live world from a partially received or unvalidated payload.

After installation, the receiver must replay events strictly beyond the snapshot's per-origin applied-through vector, using durable inbox deduplication and the existing deterministic winner order. Events that arrive during transfer must remain durable and be reconciled against the snapshot boundary. Snapshot installation and event replay must be crash-recoverable and idempotent.

This milestone does not implement any of those transfer or installation operations. They are separate follow-up work and must not be inferred from the presence of a manifest codec.


## Authenticated transfer message contract

The protocol-v2 snapshot-transfer wire codec defines the following bounded messages:

- `SNAPSHOT_OFFER`: a canonical version-1 manifest.
- `SNAPSHOT_ACCEPT` / `SNAPSHOT_REJECT`: explicit receiver decision, with a fixed rejection-reason allowlist.
- `SNAPSHOT_CHUNK`: one indexed payload segment, at most 256 KiB before Base64 framing.
- `SNAPSHOT_CHUNK_ACK`: acknowledgement of one exact chunk index.
- `SNAPSHOT_FINISH` / `SNAPSHOT_COMPLETE`: end-of-stream and receiver completion markers.

Every message includes protocol version 2 and a canonical request UUID; transfer-specific messages also bind to the manifest snapshot UUID. Chunk indices are bounded by the maximum manifest payload length. A receiver must additionally check each chunk against the active manifest: exact snapshot ID, index below the computed chunk count, and exact expected byte length (including the final chunk). The codec exposes this check as `validateChunkForManifest`.

An offer must be bound to the authenticated TLS peer identity: `manifest.sourceServerId` must exactly equal the peer ID established by the pinned-certificate HELLO/READY handshake. Its world ID must be in the receiver's configured shared-world allowlist. A valid certificate alone does not authorize arbitrary worlds or a claimed third-party snapshot origin.

Snapshot-transfer messages are wired to authenticated peer sessions. The receiver durably stages and verifies incoming bytes; the sender streams a completed local payload in one-chunk-at-a-time order and waits for the exact acknowledgement before advancing. The sender checks the payload is a regular non-symbolic-link file with the manifest's exact size and SHA-256, and applies the receiver's 512 MiB maximum. The request UUID is caller-supplied so a retry can replay from chunk zero against the receiver's durable prefix. No message contains a filesystem path, command, class name, plugin identifier, or configuration mutation.

All JSON messages are canonical and reject unknown/missing fields, duplicate keys through canonical-form rejection, malformed UTF-8, invalid UUIDs, invalid enum values, invalid Base64, oversized frames, and out-of-range chunk indices. Chunk data is copied defensively by the decoded message model.


## Receiver-side durable staging

Both peers handle snapshot frames only after the existing mutual-TLS, certificate-pinned HELLO/READY handshake. The receiver checks that the offered source ID matches the authenticated peer and that the world is configured locally. The sender can target only a currently connected, explicitly authorized peer and only a locally configured shared world. Messages are bound to the request and snapshot UUIDs held by the active session; unbound, out-of-order, or unexpected responses close the session.

The staging store is separate from all live world directories:

- Its root is the plugin-owned `snapshots/staging` directory. Payload filenames are derived only from canonical snapshot UUIDs; no remote path is accepted or interpreted.
- The store rejects symbolic links and unexpected entries in its data directory. Files are opened without following links.
- A transfer is limited to 512 MiB, four active transfers globally, one active transfer per peer, and 1 GiB of total reserved snapshot storage. These are local hard limits, not peer-controlled settings.
- Chunks must arrive in exact index order and have the manifest-derived exact size. An identical duplicate chunk is acknowledged; a conflicting duplicate is rejected.
- Each chunk is forced to the staging file before the SQLite progress record is committed. SQLite uses WAL with synchronous FULL. On restart, bytes beyond the committed contiguous prefix are truncated; a file shorter than its durable progress record fails closed.
- An offer can resume only with the same authenticated peer, request UUID, snapshot UUID, and byte-identical manifest. ACCEPT does not communicate a resume offset; the sender replays from chunk zero and the receiver recognizes and acknowledges already durable identical chunks.
- FINISH is accepted only after the exact manifest length and chunk count arrive. The receiver streams SHA-256 over the staged file and marks it COMPLETE only after the digest matches.

A successful `SNAPSHOT_COMPLETE` means **the payload is durably staged and its length and SHA-256 were verified**. It does not mean that the snapshot was installed, loaded, or applied to a world. Staged snapshots remain on disk; there is no automatic retention or pruning policy yet, and completed snapshots continue to count against the 1 GiB reservation limit. Operators should preserve the staging directory until a later installation/retention stage explicitly consumes it.

The receiver does not unzip, deserialize Java objects, load classes, execute commands, alter configuration, or write into a live world. The payload remains opaque bytes in a UUID-named staging file. The sender is available through the plugin API, but no capture command or producer currently creates completed local payloads. Live-world installation remains a later milestone.


## PAPER_BLOCK_DATA_V1 payload framing

The first payload codec is now specified and implemented by `SnapshotPayloadCodec`. It is a deterministic binary stream, not a ZIP or general archive:

| Field | Encoding |
|---|---|
| Magic | 4 bytes: ASCII `GSB1` |
| Payload version | 1 unsigned byte; currently `1` |
| Record count | 4-byte unsigned big-endian integer |
| Each record | 3 signed 32-bit big-endian coordinates (x, y, z), 2-byte unsigned big-endian UTF-8 byte length, then the BlockData bytes |

Records must be strictly sorted by ascending `x`, then `y`, then `z`. Duplicate coordinates are invalid. BlockData is limited to 1,024 UTF-8 bytes and must be a namespaced, lowercase material key with an optional simple property list. Each record has 14 bytes of framing plus its UTF-8 BlockData bytes. The record count is capped at 16,777,216. The stream must contain exactly the declared number of records and no trailing bytes.

The codec is streaming: it does not buffer the entire payload, close caller-owned streams, load classes, deserialize Java objects, or interpret paths. It validates strict UTF-8 and rejects malformed headers, unsupported versions, invalid record counts, unsorted or duplicate coordinates, malformed BlockData, truncated records, and trailing data. The writer reports the exact emitted byte length so a caller can compute the manifest length and digest while writing to a temporary file.

This defines the bytes represented by `PAPER_BLOCK_DATA_V1`; it does **not** implement a Paper world capture. A future capture service must obtain records from Bukkit/Paper safely on the server thread, avoid force-loading chunks, define which loaded/known chunks are in scope, and coordinate its event watermark with actual applied world state. No snapshot manifest should be produced until that capture boundary can be proven coherent.


## Durable world-local applied coverage

The inbound SQLite store persists each event's world-local sequence separately from its per-origin delivery sequence. A sequence is included in snapshot coverage only after the winning event's Paper-thread mutation succeeds and its inbox status is durably changed to `APPLIED`.

Coverage is reported independently for each `(world_id, origin_server_id)` pair. The contiguous watermark advances only across a gap-free prefix beginning at world-local sequence 1; received, pending, and superseded events do not advance it. The store enforces uniqueness of positive world-local sequence values within each world and origin.

Rows created before this migration receive `world_sequence = 0`. Their historical world-local order cannot be reconstructed safely from the global delivery sequence, so they are deliberately excluded from snapshot coverage. A zero watermark means no positive sequence prefix is proven applied. This conservative rule may delay snapshot eligibility, but it avoids claiming coverage for changes that may not exist in the live world.


## Incremental loaded-chunk capture primitive

The capture foundation provides a main-thread coordinator that inventories already-loaded chunks once, sorts them deterministically by chunk X/Z, and samples at most a configured number per tick (hard-capped at eight). It does not force-load or generate chunks. A chunk that unloads after inventory is skipped. The callback receives an immutable Bukkit `ChunkSnapshot` and must enqueue or copy it quickly; expensive encoding and disk I/O belong on a worker thread.

**This primitive does not produce a publishable snapshot.** Chunks are sampled across multiple ticks, so edits can occur between chunk samples. Its output must not be assigned a single world-wide applied-through vector, written as a completed manifest, transferred, or installed until the consistency/reconciliation protocol is implemented. The current block-data payload format also remains limited to block coordinates and canonical block data; it does not include block entities, inventories, entities, or biome data.

## Durable reconciliation groundwork

The received-event store now keeps an append-only applied-effect history for block mutations. A mutation is added to that history in the same SQLite transaction that marks its inbox event `APPLIED`. Later winner changes do not erase the earlier effect record. On upgrade, rows still marked `APPLIED` are backfilled; effects already lost when older rows were marked `SUPERSEDED` cannot be reconstructed, so sequence coverage remains conservative.

`SnapshotReconciler` overlays durable applied block effects newer than a supplied per-origin boundary onto a multi-tick scan. It uses the replication winner order, so a chunk scan that already contains some post-boundary edits can be reconciled idempotently. It rejects events with unknown world-local sequence numbers, malformed coordinates, duplicate coordinates, and unsupported mutation types.

This is a consistency primitive, not a complete capture transaction. A future capture service must still take the boundary on Paper's main thread, drain durable local event writes before finalizing, obtain the matching applied-event history and coverage vector, serialize to a private temporary file, verify length and SHA-256, and atomically promote the payload and manifest only after every step succeeds. Until that lifecycle is connected, the plugin does not produce or advertise complete snapshots.
