# Replication protocol notes

This document records the initial event contract and local journal behavior. It is not yet a network protocol implementation.

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

## Local journal

Each node writes accepted outgoing events to its own SQLite journal before attempting network delivery. The journal uses WAL mode and FULL synchronous commits. A successful append returns a local journal offset only after SQLite has committed the transaction.

- Event IDs are unique within a node's journal.
- The pair of origin node ID and origin sequence is also unique.
- Retrying the exact same envelope returns its original journal offset.
- Reusing an event ID or origin sequence for different data is rejected.
- The node restores its next sequence from the maximum sequence already persisted for that origin.
- Replay reads entries after a local journal offset in ascending order.

A journal offset belongs to one database. It is not comparable across regions and is not a global commit index. The current implementation does not yet prune acknowledged entries or coordinate retention across peers.

## Delivery requirements

The transport must provide authenticated peer identity, integrity protection, bounded message sizes, reconnect behavior, acknowledgements, retry, and durable replay. It must not trust a peer merely because it can reach the TCP port.

Use TLS with mutual authentication or an equivalent authenticated encrypted channel. Do not send shared secrets in plaintext or log credentials. A shared HMAC secret alone would not provide confidentiality.

## Ordering and idempotency

- Event IDs are used to recognize retries.
- Per-origin sequence numbers detect gaps from one node.
- A durable global commit offset (or a well-defined per-world partitioned log) establishes application order.
- Receivers acknowledge only after the event is durably accepted.
- Replaying an already committed event must not apply it twice.

The local SQLite journal is the durable outgoing history for this node. The in-memory EventDeduplicator remains only a bounded optimization and is not a substitute for durable receiver-side idempotency.
