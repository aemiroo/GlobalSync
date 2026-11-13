# Snapshot format and consistency contract

This document defines the metadata contract for future world-state snapshots. This milestone adds an immutable manifest and strict codec only. It does not capture world data, transfer files, stage data, install a snapshot, or change event delivery.

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
