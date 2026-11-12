# Replication protocol notes

This document records the event contract, local journal behavior, and transport foundations. It is not yet a live network protocol implementation.

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

## Transport foundation

The current transport milestone adds reusable primitives only. It does not create a listener, dial peers, send journal entries, or acknowledge delivery.

- TLS 1.3 contexts are loaded from operator-provisioned PKCS12 identity and trust stores.
- The trust store performs normal certificate-chain validation. A separate SHA-256 leaf-certificate fingerprint check binds a connected TLS identity to an explicitly authorized peer.
- Server sockets must require client certificates; the helper configures this with `needClientAuth`. Clients are restricted to TLS 1.3.
- Peer messages use a four-byte big-endian length prefix. Empty frames and frames larger than the configured maximum are rejected before payload allocation.
- The default maximum frame size is 1 MiB.
- Store passwords are expected from environment variables, not committed to config files. Never log passwords or private-key material.

The `network.enabled` setting remains false. The TLS paths, environment-variable names, and peer fingerprint map in config.yml are reserved for the next integration step and are not loaded by the plugin yet. Do not expose a port or treat a successful TLS handshake as authorization until the peer fingerprint has also been checked.

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
