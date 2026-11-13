# GlobalSync

GlobalSync is an experimental project exploring a shared Minecraft world across multiple geographically distributed Paper servers.

The intended experience is that players connect to a nearby regional server while still seeing and interacting with players in other regions. Player inventories and player sessions remain independent. The long-term design includes global chat, cross-region PvP, synchronized knockback, and replicated world state.

## Current status

**This repository contains an experimental 26.3 shared-world foundation with durable player block-edit capture, authenticated bidirectional delivery, deterministic per-block conflict selection, and opt-in remote block application. It is not yet a complete multi-region world.**

Implemented so far:

- Paper 26.3 plugin entry point and strict Minecraft version guard.
- Unique regional node identity and shared-world configuration.
- Versioned replication event envelopes.
- Local sequence generation restored from the durable journal after restart.
- SQLite append-only event journal using WAL mode and full synchronous commits.
- Idempotent retry handling and conflict detection for reused event IDs or node sequences.
- Bounded journal replay by local offset.
- Asynchronous journal append API so callers do not perform disk I/O on the Paper main thread.
- Capture of successful player block placements and breaks in configured shared worlds.
- Multi-block player placements (such as beds) are recorded as individual block changes.
- Before/after block data, world coordinates, player UUID, origin node, and sequence are stored in each event.
- Capture can be disabled with `capture.player-block-changes`.
- TLS 1.3 mutual-authentication listener and outbound connection support, loaded from operator-provisioned PKCS12 stores.
- Explicit peer ID-to-leaf-certificate SHA-256 pinning in addition to normal certificate-chain validation.
- Client-side hostname verification and a strict HELLO/READY nonce handshake.
- Bounded peer connection workers, handshake timeouts, and socket cleanup.
- Bounded length-prefixed framing (1 MiB maximum).
- Strict version-2 inbound `EVENT` allowlist for `BLOCK_PLACE` and `BLOCK_BREAK`.
- Lamport logical clocks persisted with local and received events; concurrent edits use server ID and origin sequence as deterministic tie-breakers.
- Durable per-block winner records and pending/applied/superseded inbox states.
- Opt-in Paper-main-thread application of winning remote block edits, with no forced chunk loads and no physics side effects.
- Authenticated origin and shared-world validation, with exact message and payload field sets.
- Separate SQLite inbound inbox (`journal/received.db`) with durable deduplication by event ID and origin sequence.
- Receiver ACKs only after the inbound inbox transaction commits.
- Bidirectional stop-and-wait delivery from each node's local journal, with one outstanding event per peer.
- Per-peer local journal acknowledgement cursors persisted in SQLite and resumed after restart.
- Bounded exponential reconnect attempts; timed-out sends are retried and safely deduplicated by the receiver.
- A status command and Maven/GitHub Actions verification workflow.

Authenticated peers exchange allowlisted block-edit events in both directions. Each sender advances its durable per-peer cursor only after receiving an ACK for the exact event; the receiver sends that ACK only after committing the event and its per-block winner decision. A disconnect or lost ACK causes replay from the last persisted cursor. When `application.enabled` is enabled, winning remote edits are applied on Paper's main thread. Events for unloaded chunks remain pending and are retried when the chunk is loaded.

Not implemented yet:

- Capture of environmental changes, block entities, containers, and other non-player mutations.
- Global consensus, partition fencing, and snapshot-based replica repair.
- Heartbeat/liveness messages and peer health metrics.
- Capture of explosions, pistons, fluids, redstone, growth, containers, block entities, or other non-player world changes.
- Chunk snapshots, entity simulation, and remote player rendering.
- Global chat, cross-region damage, PvP, or KnockbackSync integration.
- Global consensus and authoritative commit ordering across network partitions.

Player block edits are journaled locally and recorded in a durable per-block winner index. Event order is `(logicalClock, originServerId, originSequence)`: Lamport clocks preserve observed causal order; server ID and sequence break concurrent ties deterministically. This is deterministic last-writer-wins, not consensus. A partitioned node cannot know about edits it has not received, so conflicting writes during a partition may be resolved by the tie-breaker rather than real-world chronology. Local journal offsets and received-inbox offsets remain database-local, not global commit indexes.

## Requirements

- Minecraft **26.3 only**
- Paper **26.3**
- Java **25**
- Maven 3.9+

Paper API is compiled as a provided dependency. Paper's plugin library loader downloads the pinned SQLite JDBC and Gson versions at runtime.

## Build

```bash
mvn --batch-mode verify
```

The resulting plugin jar is in `target/`. GitHub Actions runs the same verification command on pushes and pull requests.

## Install

1. Build the project with Java 25.
2. Copy `target/globalsync-0.1.0-SNAPSHOT.jar` into the Paper server's `plugins` directory.
3. Start the server once to generate `plugins/GlobalSync/config.yml` and the local journal database.
4. Set a unique `server-id` for each region.
5. Ensure the intended world names appear under `shared-worlds`.
6. Leave `capture.player-block-changes: true` to journal player block edits, or set it to `false` to disable capture.
7. Keep `network.enabled: false` unless you are deliberately testing authenticated peer replication between controlled nodes.
8. Keep `application.enabled: false` until all peers run protocol v2 and you are testing against backed-up worlds. Enable it only on every node in the test topology.

### Controlled peer-replication testing

The network and world-application layers are independently opt-in. The network sends and receives allowlisted block-edit events with durable acknowledgements. With `application.enabled: true`, winning edits are applied on the Paper main thread; unloaded chunks are not force-loaded.

Before enabling it:

1. Provision a unique PKCS12 identity store for each node and a trust store containing the peer certificate chains that node should trust.
2. Place both stores under that node's `plugins/GlobalSync/` data directory. The default paths are `tls/node.p12` and `tls/truststore.p12`.
3. Set the environment variables named by `identity-store-password-env` and `trust-store-password-env` in the server process environment. Do not put passwords in YAML or source control.
4. Configure the peer under `network.peers.<remote-server-id>` with its DNS name or IP, listening port, and SHA-256 fingerprint of its leaf certificate.
5. Configure the reciprocal peer entry on the other node. The configured host must match a DNS name or IP SAN in the remote certificate.
6. Set `network.bind-address` to the intended local interface, configure the same listening port on both nodes as appropriate, and restrict firewall access to known regional nodes.
7. Only then set `network.enabled: true` on both nodes and restart.

Each pair uses the lexicographically lower server ID as the outbound dialer. Both nodes must authorize one another by ID and certificate fingerprint. Invalid certificates, unknown IDs, handshake failures, and excess connections are rejected. If the configuration or TLS stores are invalid, GlobalSync disables itself rather than starting with weaker security.

**Do not expose this as a production replication network.** This milestone only handles player placements and breaks; it does not capture environmental simulation, containers, block entities, or entity state. The Lamport ordering and tie-breaker provide deterministic conflict selection but do not provide consensus or partition fencing. Upgrade every peer to protocol v2 together. Remote commands, file access, plugin loading, and remote configuration changes are not part of the protocol. Keep networking and application disabled outside controlled tests.

The local outgoing event database is stored at `plugins/GlobalSync/journal/events.db`; the durable inbound inbox is `plugins/GlobalSync/journal/received.db`. Back them up with the server stopped or by using a SQLite-aware backup method.

Use `/globalsync status` to inspect the current node, capture state, journal offset, and authenticated peer count.

See [the architecture notes](docs/ARCHITECTURE.md), [the protocol notes](docs/PROTOCOL.md), and [the security policy](SECURITY.md) for implementation boundaries and operational requirements.
