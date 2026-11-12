# Replication protocol notes

This document records the event contract, local journal behavior, and transport foundations. The authenticated connection handshake is implemented, but replication messages are not yet implemented.

## Envelope

Every replicated event should carry:

- Protocol version.
- Globally unique event ID.
- Shared world identifier.
- Origin node identifier.
- Origin sequence number.
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

This first capture pass intentionally does not include explosions, piston movement, fluids, redstone, growth, containers, block entities, or other environmental changes. It also does not yet transmit or apply captured events on another server.

## Local journal

Each node writes accepted outgoing events to its own SQLite journal before attempting network delivery. The journal uses WAL mode and FULL synchronous commits. A successful append returns a local journal offset only after SQLite has committed the transaction.

- Event IDs are unique within a node's journal.
- The pair of origin node ID and origin sequence is also unique.
- Retrying the exact same envelope returns its original journal offset.
- Reusing an event ID or origin sequence for different data is rejected.
- The node restores its next sequence from the maximum sequence already persisted for that origin.
- Replay reads entries after a local journal offset in ascending order.

A journal offset belongs to one database. It is not comparable across regions and is not a global commit index. The current implementation does not yet prune acknowledged entries or coordinate retention across peers.

## Authenticated peer connection

The current connection layer establishes an authenticated, idle TLS channel only. It does not send journal entries, receive replication events, acknowledge delivery, or apply remote world changes.

- TLS 1.3 is required; the server requires a client certificate.
- The TLS trust store validates the certificate chain. After the TLS handshake, the remote node must also present a leaf certificate whose SHA-256 fingerprint matches the configured fingerprint for its claimed server ID.
- The client verifies the server certificate hostname against the configured peer host.
- Both sides exchange a strict protocol-version-1 HELLO containing a node ID and fresh UUID nonce. Each side validates the other node's configured certificate pin, then exchanges READY messages echoing the opposite nonce.
- Unknown JSON fields, missing fields, incorrect types, invalid nonces, unsupported protocol versions, unconfigured peer IDs, and incorrect certificate pins are rejected.
- Only one side dials each peer pair: the lexicographically lower server ID initiates the outbound connection. This avoids simultaneous duplicate connections when both nodes list one another.
- Inbound and outbound connections share a bounded worker pool. Excess connections are rejected. TLS handshakes have a configured read timeout.
- After authentication, the socket is held open so disconnects can be detected. Any post-handshake application data is currently rejected because no application-message allowlist exists yet.

Configure each remote peer under `network.peers.<server-id>` with `host`, `port`, and the SHA-256 fingerprint of its leaf certificate. The host must match a DNS name or IP address in the peer certificate's subject alternative name. Each node must also have its own PKCS12 identity store and a trust store containing the certificates or issuing CA certificates it is configured to trust.

The `network.enabled` setting remains `false` by default. Enabling it opens the TLS listener and initiates configured peer handshakes; it does not enable replication. Use it only in a controlled test network with operator-provisioned certificates and firewall restrictions.

## Delivery requirements

The eventual transport must provide authenticated peer identity, integrity protection, bounded message sizes, reconnect behavior, acknowledgements, retry, and durable replay. It must not trust a peer merely because it can reach the TCP port.

Receivers acknowledge only after the event is durably accepted. Shared secrets must never be sent in plaintext. TLS with mutual authentication and explicit peer authorization is the selected baseline.

## Ordering and idempotency

- Event IDs are used to recognize retries.
- Per-origin sequence numbers detect gaps from one node.
- A durable global commit offset (or a well-defined per-world partitioned log) establishes application order.
- Replaying an already committed event must not apply it twice.
- Receiver-side deduplication must survive process restarts.

The local SQLite journal is the durable outgoing history for this node. The in-memory EventDeduplicator remains only a bounded optimization and is not a substitute for durable receiver-side idempotency.
