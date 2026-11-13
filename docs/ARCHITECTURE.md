# Architecture

## Target topology

Each region runs a Paper 26.3 server and keeps a complete local copy of the shared world. Players connect to the region with the best route from their location. Player inventories and player sessions remain independent. The long-term design includes global chat, cross-region PvP, synchronized knockback, and replicated world state.

A complete copy on every node does not mean every node should independently simulate every object. That would cause divergent mob AI, random ticks, fluids, redstone, and entity movement.

## Consistency model

The block-edit model is merged ownership: every connected node may author edits to the same shared world. There are no fixed per-region chunk ownership boundaries. Operations outside this narrow block-edit model (especially entity simulation) will still need explicit authority.

A future implementation must define:

1. A durable event log and monotonic committed offsets.
2. An authority/lease model for entity simulation and conflicting world mutations.
3. Chunk versions and snapshot boundaries.
4. Duplicate suppression that survives process restarts.
5. Backpressure and bounded queues during regional outages.
6. Recovery from a known committed offset, with snapshot fallback.
7. A clear policy for partitions: pause or reject writes that cannot be safely ordered instead of allowing split-brain state.

Each node has a local SQLite journal with a monotonically increasing local offset. That offset is useful for replaying one node's journal, but it is not a global sequence and does not imply replication or commitment by another region. The per-node event sequence is restored from this journal after restart.

## Shared block ownership and conflict resolution

All nodes may accept player block placements and breaks in the configured shared worlds. Each event carries a persisted Lamport logical clock. A node increments its clock for a local edit and advances beyond a remote clock when it accepts that event. For competing edits to one coordinate, the winner is the maximum tuple `(logicalClock, originServerId, originSequence)`. The server ID and sequence provide a stable tie-break for concurrent events. Wall-clock timestamps are diagnostic only.

The inbox persists every event, its state, and the current winner for each world coordinate. A late lower-ranked event cannot replace a higher-ranked winner. A queued application task rechecks the durable winner while holding the inbox lock, so a newer winner cannot be committed between that check and the block mutation. Replaying an event after a crash is safe: setting the same block data is idempotent, and the event remains pending if the process stops before its applied state is committed.

This is deterministic last-writer-wins, not a consensus protocol. A node isolated by a network partition cannot observe remote edits and cannot assign clocks based on them. Conflicts created during a partition may therefore be decided by the stable tie-breaker rather than actual chronology. Production use still requires partition policy, snapshot repair, and broader environmental-state capture.

## Capture boundary

The first Paper event integration records successful player block placements and breaks in configured shared worlds. Multi-block placements are expanded into individual location-specific events. Event data is copied into immutable strings and UUIDs on the server thread; SQLite persistence is queued to the dedicated journal worker.

This does not yet cover environmental simulation, block entities, inventories, redstone, fluids, pistons, explosions, or any other indirect world changes. It is a capture milestone, not a complete world replication model.

## Threading

Bukkit/Paper world access must happen on the server's appropriate scheduler/thread. Network reads and writes must run off the main thread. Incoming messages are validated and durably recorded off-thread. The network worker only schedules application. Winning block changes are applied on the Paper main thread, only when the target world and chunk are already loaded; chunks are never force-loaded. Physics is disabled to avoid generating unjournaled neighbor updates.

Journal writes are submitted to a dedicated single-thread executor. Callers receive a CompletableFuture that completes after the SQLite append has committed. The executor preserves append order for submissions made through the plugin API. Peer senders read committed entries from the journal and persist independent per-peer acknowledgement cursors in SQLite.

## Transport security and peer lifecycle

The transport now has an opt-in TLS listener and outbound dialer. TLS 1.3 and mutual certificate authentication are mandatory. After the TLS handshake, each side must claim a configured node ID, prove possession of the matching pinned leaf certificate, and complete a nonce-based HELLO/READY exchange. The client also verifies the configured hostname against the certificate SAN.

The listener and outbound connections use a bounded worker pool. Excess connections are rejected, handshakes have read timeouts, and shutdown closes active and pending sockets. A deterministic node-ID ordering chooses one dialer per peer pair to avoid simultaneous duplicate connections. Authenticated inbound EVENT frames pass through bounded per-peer message-count and byte token buckets before parsing or durable storage; bucket state is capped at 64 peers.

Authenticated sessions exchange nonce-matched PING/PONG frames after an idle interval. A missed PONG closes the socket and lets the existing deterministic reconnect loop restore the session; heartbeat traffic is bounded separately from replication EVENT traffic. After authentication, the peer worker accepts only the implemented versioned protocol families: version-2 `EVENT` frames for player block placements and breaks, targeted gap-repair messages tied to an outstanding request, nonce-validated heartbeat control frames, and snapshot-transfer messages tied to an active peer session/request. Event handling validates the claimed origin against the authenticated peer, checks the shared-world allowlist and exact payload schema, and commits the event and per-block winner decision to a separate SQLite inbox. The ACK is sent only after that FULL-synchronous transaction commits. The inbox deduplicates by event ID and origin sequence across restarts. Snapshot offers and chunks are handled separately by the bounded staging store; payloads remain opaque and are not extracted, deserialized, loaded, or installed into a live world. Each side also runs a bounded stop-and-wait sender over the same full-duplex connection. It sends the next local journal entry, waits for a matching event-ID ACK, and then commits that peer's local journal cursor. Cursor updates are checked against the exact next row and survive restart. ACK timeouts close the session; the deterministic dialer retries with bounded exponential backoff, and unacknowledged events replay safely through receiver-side deduplication. When separately enabled, the Paper-thread application service applies the current winner and leaves events pending while their world or chunk is unavailable. `network.enabled` remains false by default.

## Replication layers

- **Presence:** online node registry and remote player metadata.
- **Player view:** remote player spawn, movement interpolation, equipment, metadata, and removal.
- **World events:** block and block-entity changes, containers, explosions, fluids, redstone, and scheduled updates.
- **Chunk state:** versioned snapshots and incremental updates.
- **Entities:** single-writer simulation authority with replicated state.
- **Combat:** one authoritative hit/damage decision, with the outcome and velocity replicated to both regions.
- **World metadata:** time, weather, gamerules, and world border.

## Peer liveness

A single daemon scheduler checks active sessions. When a session has received no frames for the configured interval, it sends one nonce-bearing PING. The reader accepts only a matching PONG, responds to peer PINGs, and counts valid responses. If the PONG deadline expires, the session is closed; durable delivery resumes from the last acknowledged journal cursor after reconnect. Heartbeat messages do not touch the event inbox or journal. Defaults are a 15-second idle interval and a 45-second response timeout.

## Replica progress visibility

The local journal can produce a point-in-time delivery snapshot for each configured peer: this node's journal head offset, that peer's durable acknowledgement cursor, the number of still-pending local events, and the oldest pending event timestamp. The connection manager adds whether an authenticated session is currently open and the valid PONG count for that active session. `/globalsync status` reports `DISCONNECTED`, `CATCHING_UP`, or `CURRENT` from those facts. A disconnected peer is never reported current, even when its local backlog is empty.

These are outbound-delivery diagnostics only. A successful ACK means the remote inbox durably accepted the event; it does not prove that the remote Paper world applied it. Local offsets are not comparable between nodes, and this status view does not detect missing remote-origin events or prove world equivalence. The status command performs its SQLite reads asynchronously and sends the result back on the Paper server thread.

## Origin sequence-gap diagnostics

The durable inbox groups received events by origin and scans each origin's sequence numbers from one through its highest observed sequence. If a later sequence exists while an earlier one is absent, `/globalsync status` reports the first missing sequence, highest observed sequence, and received event count. When the missing event later arrives through the existing ordered journal replay, the gap clears automatically. This is receive-side coverage over events retained in the local inbox; it does not infer gaps from local journal offsets or wall-clock timestamps.

A reported gap is evidence that this replica has not durably received a sequence that a later event proves exists. It does not identify whether the cause is an in-flight event, an interrupted connection, or missing sender history. This milestone does not request selective retransmission, change ACK behavior, or repair world state. A replica with a gap must not be described as fully synchronized.

## Failure behavior

A regional partition must not silently permit conflicting writes to the same authoritative state. Nodes should track their last committed offset, detect gaps, and resynchronize from the durable log or a snapshot. A disconnected region may continue read-only, or accept only operations explicitly safe to queue, until it can rejoin without creating split-brain state.

## Plugin boundary

The current plugin is intentionally a thin Paper integration. Protocol types and replication primitives should remain independent of Bukkit classes so they can be tested without starting a Minecraft server. If full-fidelity capture of internal simulation changes cannot be achieved through stable Paper APIs, a narrowly scoped server integration layer may be required.


## Snapshot boundary contract

Snapshot metadata is defined in [SNAPSHOTS.md](SNAPSHOTS.md). A manifest describes one world, one immutable payload, and a per-origin vector of contiguous applied event sequences represented by that payload. These values are not local journal offsets or a global commit position. A received-event ACK is not sufficient to advance a snapshot watermark because it confirms durable inbox acceptance, not world application.

The current milestone provides only the immutable manifest model and strict canonical JSON codec. It does not capture, transfer, stage, or install world data. A future capture must coordinate with Paper's server thread and block application to establish a coherent state boundary, then replay events beyond the vector after installation. The current player-block-edit-only capture boundary remains a major limitation; this is not a full Minecraft world snapshot.


### Incremental chunk capture

`IncrementalChunkCapture` is a bounded Paper-thread capture primitive. It snapshots only chunks loaded at the beginning of a capture, processes no more than the configured per-tick budget (maximum eight), sorts chunk coordinates for deterministic traversal, and skips chunks that unload before sampling. It never calls a chunk-loading or generation API. Captured `ChunkSnapshot` values are immutable hand-off objects; consumers must keep the main-thread callback short and perform serialization/storage asynchronously.

This is not yet a consistent world snapshot. The multi-tick scan has no atomic world boundary and must not be published with a single snapshot watermark. Event reconciliation, durable output staging, cancellation policy, and a verified completion manifest remain later steps.
