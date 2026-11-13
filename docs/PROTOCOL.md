# Replication protocol notes

This document records the event contract, local journal behavior, authenticated transport, and the first inbound replication-message implementation.

## Envelope

Every replicated event should carry:

- Protocol version.
- Globally unique event ID.
- Shared world identifier.
- Origin node identifier.
- Origin sequence number.
- Lamport logical clock for deterministic conflict ordering.
- Creation timestamp for diagnostics (not for ordering).
- Event type.
- Typed payload.

The ReplicationEnvelope record captures this metadata. Its attributes map is a temporary lightweight placeholder; production events should use typed, schema-versioned payloads.

## Captured player block edits

The current Bukkit integration captures successful player block placements and breaks in worlds listed under `shared-worlds`.

- `BLOCK_PLACE` records the replaced block data and the resulting placed block data.
- `BLOCK_BREAK` records the previous block data and the expected `minecraft:air` result.
- Every event includes the world-local integer coordinates and the acting player's UUID.
- `BlockMultiPlaceEvent` is expanded into one event per replaced block, so multi-block placements such as beds are not reduced to a single coordinate.
- Handlers run at MONITOR priority and ignore cancelled events. They do not change the world or cancel gameplay.
- Block data is serialized using Bukkit's block-data string representation.

This first capture pass intentionally does not include explosions, piston movement, fluids, redstone, growth, containers, block entities, or other environmental changes. Captured events are journaled locally; outbound sending and remote world application are not implemented.

## Local journal

Each node writes accepted outgoing events to its own SQLite journal before attempting network delivery. The journal uses WAL mode and FULL synchronous commits. A successful append returns a local journal offset only after SQLite has committed the transaction.

- Event IDs are unique within a node's journal.
- The pair of origin node ID and origin sequence is also unique.
- Retrying the exact same envelope returns its original journal offset.
- Reusing an event ID or origin sequence for different data is rejected.
- The node restores its next sequence from the maximum sequence already persisted for that origin. Existing v1 outgoing journal rows are migrated to v2 with a bounded logical-clock seed derived from their origin sequence. Existing v1 inbox rows are migrated to canonical v2 payloads and inserted into the winner index so an unacknowledged replay is still recognized as the same event.
- Replay reads entries after a local journal offset in ascending order.

A journal offset belongs to one database. It is not comparable across regions and is not a global commit index. The current implementation does not yet prune acknowledged entries or coordinate retention across peers.

## Authenticated peer connection

After the authenticated HELLO/READY handshake, the connection can receive version-2 EVENT frames. The transport HELLO/READY handshake remains protocol version 1; the application EVENT/ACK envelope is version 2. The receiver validates the authenticated origin, shared-world allowlist, event type, exact JSON field sets, and block-edit payload before storing the event and deterministic per-block winner decision in a separate durable inbox. It sends an ACK containing the event ID and receiver-local inbox offset only after the FULL-synchronous transaction commits. The sender reads the local journal in offset order, sends one event per peer at a time, and advances its durable per-peer cursor only after a matching ACK is received. The ACK's `journalOffset` is the receiver's inbox offset; the sender separately persists the corresponding local journal offset.

- TLS 1.3 is required; the server requires a client certificate.
- The TLS trust store validates the certificate chain. After the TLS handshake, the remote node must also present a leaf certificate whose SHA-256 fingerprint matches the configured fingerprint for its claimed server ID.
- The client verifies the server certificate hostname against the configured peer host.
- Both sides exchange a strict protocol-version-1 HELLO containing a node ID and fresh UUID nonce. Each side validates the other node's configured certificate pin, then exchanges READY messages echoing the opposite nonce.
- Unknown JSON fields, missing fields, incorrect types, invalid nonces, unsupported protocol versions, unconfigured peer IDs, and incorrect certificate pins are rejected.
- Only one side dials each peer pair: the lexicographically lower server ID initiates the outbound connection. This avoids simultaneous duplicate connections when both nodes list one another.
- Inbound and outbound connections share a bounded worker pool. Excess connections are rejected. TLS handshakes have a configured read timeout.
- EVENT messages are currently limited to `BLOCK_PLACE` and `BLOCK_BREAK`; their payload must contain exactly the player UUID, integer coordinates, previous block data, and resulting block data.
- The receiver binds `originServerId` to the authenticated TLS peer ID and checks the world against `shared-worlds`.
- The receiver inbox is stored separately at `journal/received.db` with WAL mode and FULL synchronous commits. Event ID and `(origin node, origin sequence)` are unique keys. Exact retries return the original inbox offset; conflicting reuse is rejected. The inbox also stores pending/applied/superseded states, a Lamport clock high-water mark, and a per-block winner index.
- ACK messages contain only the protocol version, event ID, and receiver-local durable inbox offset. They are not global offsets and confirm durable receipt/winner selection, not successful application to the Minecraft world.
- Each node independently sends its own journal over the same full-duplex TLS connection. A single reader dispatches EVENT and ACK frames; writes are serialized.
- Delivery is stop-and-wait: at most one event per peer is awaiting acknowledgement. This intentionally bounds in-flight memory and preserves journal order.
- The sender persists a per-peer cursor in the local `events.db` database. It advances only for the exact next local journal row and only after an ACK with the expected event ID. The receiver's inbox offset is stored as diagnostic metadata, never as the sender's cursor.
- If an ACK times out or the connection drops, the session closes and the deterministic dialer reconnects with bounded exponential backoff. The sender resumes from its persisted cursor; an event committed by the receiver but whose ACK was lost is replayed and deduplicated before the receiver ACKs it again.
- Other application message types, unknown fields, malformed values, unsupported versions, and unsupported event types are rejected.

Configure each remote peer under `network.peers.<server-id>` with `host`, `port`, and the SHA-256 fingerprint of its leaf certificate. The host must match a DNS name or IP address in the peer certificate's subject alternative name. Each node must also have its own PKCS12 identity store and a trust store containing the certificates or issuing CA certificates it is configured to trust.

The `network.enabled` setting remains `false` by default. Enabling it opens the TLS listener, initiates configured peer handshakes, and enables bidirectional durable event delivery. Remote application is separately controlled by `application.enabled`, which also defaults to `false`. Use both only in a controlled test network with operator-provisioned certificates and firewall restrictions.

## Delivery requirements

Outbound delivery, durable per-peer cursors, stop-and-wait retry, and reconnect replay are now implemented. Remaining transport work includes explicit rate limiting, operational metrics, and broader fault-injection coverage. The optional application service applies winning events on the Paper main thread. It does not force-load chunks, disables block physics, and leaves events pending when their world or chunk is unavailable. It must not trust a peer merely because it can reach the TCP port.

Receivers acknowledge only after the event is durably accepted. Shared secrets must never be sent in plaintext. TLS with mutual authentication and explicit peer authorization is the selected baseline.

## Ordering and idempotency

Each event includes a positive Lamport logical clock. Local events increment the node clock; accepting a remote event advances the clock beyond the remote value. The per-block winner is selected by the total order `(logicalClock, originServerId, originSequence)`. This makes the winner independent of network arrival order for a fixed set of events. Lamport clocks capture only causality a node has observed: events created independently during a partition can be concurrent, and the stable server-ID/sequence tie-breaker decides their winner. This is deterministic last-writer-wins, not consensus or real-time ordering.

The inbox persists a winner per world coordinate. A stale event cannot replace a higher-ranked winner. Application rechecks the winner while holding the inbox lock, applies block data on the Paper main thread, and only then marks the event applied. If the process stops after the block mutation but before the status update, replay sets the same block data again. Only already-loaded chunks are eligible; unloaded chunks remain pending.

## Ordering and idempotency

- Event IDs are used to recognize retries.
- Per-origin sequence numbers provide uniqueness and origin-local identity. Explicit sequence-gap detection is not implemented yet.
- A durable global commit offset (or a well-defined per-world partitioned log) establishes application order.
- Replaying an already committed event must not apply it twice.
- Receiver-side deduplication must survive process restarts. The inbound SQLite inbox currently enforces this for received block-edit events.

The local SQLite journal is the durable outgoing history for this node. Per-peer acknowledgement cursors are stored in the same database and survive restart. The in-memory EventDeduplicator remains only a bounded optimization and is not a substitute for durable receiver-side idempotency.
