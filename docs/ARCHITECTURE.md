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

The listener and outbound connections use a bounded worker pool. Excess connections are rejected, handshakes have read timeouts, and shutdown closes active and pending sockets. A deterministic node-ID ordering chooses one dialer per peer pair to avoid simultaneous duplicate connections.

After authentication, the peer worker accepts only version-1 `EVENT` frames for player block placements and breaks. It validates the claimed origin against the authenticated peer, checks the shared-world allowlist and exact payload schema, and writes the event to a separate SQLite inbox. The ACK is sent only after the FULL-synchronous transaction commits. The inbox deduplicates by event ID and origin sequence across restarts. Each side also runs a bounded stop-and-wait sender over the same full-duplex connection. It sends the next local journal entry, waits for a matching event-ID ACK, and then commits that peer's local journal cursor. Cursor updates are checked against the exact next row and survive restart. ACK timeouts close the session; the deterministic dialer retries with bounded exponential backoff, and unacknowledged events replay safely through receiver-side deduplication. There is still no Minecraft world application. `network.enabled` remains false by default.

## Replication layers

- **Presence:** online node registry and remote player metadata.
- **Player view:** remote player spawn, movement interpolation, equipment, metadata, and removal.
- **World events:** block and block-entity changes, containers, explosions, fluids, redstone, and scheduled updates.
- **Chunk state:** versioned snapshots and incremental updates.
- **Entities:** single-writer simulation authority with replicated state.
- **Combat:** one authoritative hit/damage decision, with the outcome and velocity replicated to both regions.
- **World metadata:** time, weather, gamerules, and world border.

## Failure behavior

A regional partition must not silently permit conflicting writes to the same authoritative state. Nodes should track their last committed offset, detect gaps, and resynchronize from the durable log or a snapshot. A disconnected region may continue read-only, or accept only operations explicitly safe to queue, until it can rejoin without creating split-brain state.

## Plugin boundary

The current plugin is intentionally a thin Paper integration. Protocol types and replication primitives should remain independent of Bukkit classes so they can be tested without starting a Minecraft server. If full-fidelity capture of internal simulation changes cannot be achieved through stable Paper APIs, a narrowly scoped server integration layer may be required.
