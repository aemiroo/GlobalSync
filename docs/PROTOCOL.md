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

This first capture pass intentionally does not include explosions, piston movement, fluids, redstone, growth, containers, block entities, or other environmental changes. Captured events are journaled locally and can be delivered to authenticated peers; remote world application remains separately opt-in.

## Local journal

Each node writes accepted outgoing events to its own SQLite journal before attempting network delivery. The journal uses WAL mode and FULL synchronous commits. A successful append returns a local journal offset only after SQLite has committed the transaction.

- Event IDs are unique within a node's journal.
- The pair of origin node ID and origin sequence is also unique.
- Retrying the exact same envelope returns its original journal offset.
- Reusing an event ID or origin sequence for different data is rejected.
- The node restores its next sequence from the maximum sequence already persisted for that origin. Existing v1 outgoing journal rows are migrated to v2 with a bounded logical-clock seed derived from their origin sequence. Existing v1 inbox rows are migrated to canonical v2 payloads and inserted into the winner index so an unacknowledged replay is still recognized as the same event.
- Replay reads entries after a local journal offset in ascending order.
- The journal exposes an exact `(origin_server_id, origin_sequence)` lookup for targeted recovery. It returns only a row already present in this node's own durable journal; it does not expose a network request path or authorize callers.

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
- Before JSON parsing or SQLite access, each authenticated peer is subject to a bounded in-memory token bucket: at most 100 EVENT frames per second with a burst of 200, and at most 1 MiB of payload bytes per second with a 2 MiB burst. The limiter tracks at most 64 peer buckets; it does not create per-message tasks or queues. Exceeding either budget closes the current peer session, after which normal bounded reconnect and durable replay apply.
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

## Peer progress diagnostics

`/globalsync status` reports each configured peer as `DISCONNECTED`, `CATCHING_UP`, or `CURRENT`. The state is derived from the active authenticated session and this node's durable outbound cursor: a connected peer with pending local journal rows is catching up; a connected peer with no pending rows is current; a missing/closed session is disconnected. The report also includes this node's journal head and acknowledged offsets, pending row count, oldest pending event timestamp, and valid heartbeat PONG count for the active session.

These offsets belong only to the reporting node's `events.db`. The remote receipt offset remains diagnostic metadata and is never used as the local cursor. This view measures outbound delivery, not remote world application or completeness of the remote replica. ACK confirms durable inbox acceptance only.

## Origin sequence-gap diagnostics

The receiver derives per-origin sequence coverage from the durable `received_events` inbox. For each origin with at least one stored event, it checks sequence numbers beginning at one and reports the first absent sequence only when a higher sequence has already been received. A contiguous origin reports coverage through its highest observed sequence. The `/globalsync status` output includes the received count and highest observed sequence so operators can distinguish a small gap from a larger backlog.

The existing sender uses stop-and-wait journal delivery in origin sequence order, so ordinary reconnect replay should fill a transient gap. The diagnostic does not send a gap request, selectively retransmit, or change protocol messages. It cannot detect a missing tail event when no later sequence has arrived, and it cannot prove that the remote world applied received events. Inbox history is currently retained, which is required for this coverage calculation.

## Selective gap-repair message format

Protocol-v2 now defines strict `GAP_REQUEST`, `GAP_EVENT`, and `GAP_UNAVAILABLE` wire payloads. A request carries a fresh request UUID plus one exact origin server ID and positive origin sequence. An event response carries the original canonical EVENT payload unchanged; an unavailable response identifies the exact requested origin/sequence and uses the bounded `NOT_RETAINED` reason.

These message encoders/decoders are protocol primitives only at this stage. They are not yet dispatched by the peer connection manager and do not trigger automatic requests or retransmission. When network dispatch is added, a receiver must bind the requested origin ID to the local node's own server ID before calling `findByOriginSequence`; requests for another origin must be rejected. A returned event must still pass the normal authenticated-origin, shared-world, payload, rate-limit, durable-inbox, and deduplication checks. The exact request UUID is for correlating a later response, not an authorization credential.

## Authenticated selective gap repair

When an authenticated peer's EVENT creates a durable origin-sequence hole, the receiver requests the first missing sequence from that same peer. The request is scoped to the authenticated peer ID and contains one positive origin sequence plus a fresh request UUID. The origin accepts a request only when its `originServerId` exactly equals the origin node's own configured server ID, then performs the exact local journal lookup introduced earlier. It returns the original EVENT envelope or `GAP_UNAVAILABLE` when that sequence is no longer retained.

A targeted `GAP_EVENT` must match the outstanding request UUID and exact sequence. It passes through the same authenticated-origin, shared-world, payload-schema, inbound rate-limit, Lamport-clock, durable inbox, deduplication, winner-selection, and application scheduling path as an ordinary EVENT. The receiver sends `GAP_RECEIVED` only after that durable acceptance. Targeted replay does not create or advance the ordinary per-peer journal ACK cursor. Requests are stop-and-wait per peer, retried with a bounded count, and rate-limited; gap response state is bounded to one outstanding response per session. A missing retained event is logged for operator-assisted recovery; this stage does not synthesize history or transfer snapshots.

The gap-repair frames use the existing authenticated TLS peer session and protocol version 2. A request UUID is only a correlation value. Peers cannot query another origin's journal, request arbitrary files, or invoke remote commands. A sequence gap is requested only when a later event from that origin has already been durably received; a missing tail event remains undetectable until a higher sequence arrives.

## Delivery requirements

Outbound delivery, durable per-peer cursors, stop-and-wait retry, and reconnect replay are now implemented. Heartbeat/liveness messages and a basic PONG counter are implemented. Remaining transport work includes broader operational metrics, configurable rate limits, and fault-injection coverage. The optional application service applies winning events on the Paper main thread. It does not force-load chunks, disables block physics, and leaves events pending when their world or chunk is unavailable. It must not trust a peer merely because it can reach the TCP port.

Receivers acknowledge only after the event is durably accepted. Shared secrets must never be sent in plaintext. TLS with mutual authentication and explicit peer authorization is the selected baseline.

## Ordering and idempotency

Each event includes a positive Lamport logical clock. Local events increment the node clock; accepting a remote event advances the clock beyond the remote value. The per-block winner is selected by the total order `(logicalClock, originServerId, originSequence)`. This makes the winner independent of network arrival order for a fixed set of events. Lamport clocks capture only causality a node has observed: events created independently during a partition can be concurrent, and the stable server-ID/sequence tie-breaker decides their winner. This is deterministic last-writer-wins, not consensus or real-time ordering.

The inbox persists a winner per world coordinate. A stale event cannot replace a higher-ranked winner. Application rechecks the winner while holding the inbox lock, applies block data on the Paper main thread, and only then marks the event applied. If the process stops after the block mutation but before the status update, replay sets the same block data again. Only already-loaded chunks are eligible; unloaded chunks remain pending.

## Ordering and idempotency

- Event IDs are used to recognize retries.
- Per-origin sequence numbers provide uniqueness and origin-local identity. The durable inbox now reports the first missing sequence when a later sequence from that origin has already been received. This is diagnostic only and does not yet trigger selective replay or snapshot recovery.
- A durable global commit offset (or a well-defined per-world partitioned log) establishes application order.
- Replaying an already committed event must not apply it twice.
- Receiver-side deduplication must survive process restarts. The inbound SQLite inbox currently enforces this for received block-edit events.

The local SQLite journal is the durable outgoing history for this node. Per-peer acknowledgement cursors are stored in the same database and survive restart. The in-memory EventDeduplicator remains only a bounded optimization and is not a substitute for durable receiver-side idempotency.
