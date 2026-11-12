# Replication protocol notes

This document records the initial event contract. It is not yet a network protocol implementation.

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

## Delivery requirements

The transport must provide authenticated peer identity, integrity protection, bounded message sizes, reconnect behavior, acknowledgements, retry, and durable replay. It must not trust a peer merely because it can reach the TCP port.

Use TLS with mutual authentication or an equivalent authenticated encrypted channel. Do not send shared secrets in plaintext or log credentials. A shared HMAC secret alone would not provide confidentiality.

## Ordering and idempotency

- Event IDs are used to recognize retries.
- Per-origin sequence numbers detect gaps from one node.
- A durable global commit offset (or a well-defined per-world partitioned log) establishes application order.
- Receivers acknowledge only after the event is durably accepted.
- Replaying an already committed event must not apply it twice.

The in-memory EventDeduplicator is not a substitute for a durable event store and resets when the server restarts.
