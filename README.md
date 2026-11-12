# GlobalSync

GlobalSync is an experimental project exploring a shared Minecraft world across multiple geographically distributed Paper servers.

The intended experience is that players connect to a nearby regional server while still seeing and interacting with players in other regions. Player inventories and player sessions remain independent. The long-term design includes global chat, cross-region PvP, synchronized knockback, and replicated world state.

## Current status

**This repository contains a 26.3 plugin foundation, a durable local event journal, initial player block-edit capture, and an opt-in authenticated peer layer with a strict inbound event protocol. It is not yet a working multi-region world.**

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
- Strict version-1 inbound `EVENT` allowlist for `BLOCK_PLACE` and `BLOCK_BREAK`.
- Authenticated origin and shared-world validation, with exact message and payload field sets.
- Separate SQLite inbound inbox (`journal/received.db`) with durable deduplication by event ID and origin sequence.
- Receiver ACKs only after the inbound inbox transaction commits.
- A status command and Maven/GitHub Actions verification workflow.

The peer layer now accepts and durably acknowledges allowlisted inbound block-edit events after authentication. It does not yet send this node's local journal entries or apply received events to the Minecraft world.

Not implemented yet:

- Outbound event delivery from the local journal, per-peer ACK cursors, retries, and reconnect replay.
- Applying durably received block changes on another server.
- Applying incoming block changes on another server.
- Rollback or compensation if a remote region disagrees with a local edit.
- Automatic peer reconnection and heartbeat/liveness messages.
- Capture of explosions, pistons, fluids, redstone, growth, containers, block entities, or other non-player world changes.
- Chunk snapshots, entity simulation, and remote player rendering.
- Global chat, cross-region damage, PvP, or KnockbackSync integration.
- Global commit ordering, consensus, conflict resolution, or replica repair.

Player block edits are journaled locally. Inbound block events from authenticated peers can be durably stored and acknowledged, but local events are not yet transmitted and received events are not applied to the world. Local journal offsets and received-inbox offsets are database-local, not global commit indexes.

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
7. Keep `network.enabled: false` unless you are deliberately testing the authenticated connection layer.

### Controlled peer-connection testing

The network layer is opt-in. It can durably receive and acknowledge allowlisted block-edit events, but does not send local events or apply received changes to world state.

Before enabling it:

1. Provision a unique PKCS12 identity store for each node and a trust store containing the peer certificate chains that node should trust.
2. Place both stores under that node's `plugins/GlobalSync/` data directory. The default paths are `tls/node.p12` and `tls/truststore.p12`.
3. Set the environment variables named by `identity-store-password-env` and `trust-store-password-env` in the server process environment. Do not put passwords in YAML or source control.
4. Configure the peer under `network.peers.<remote-server-id>` with its DNS name or IP, listening port, and SHA-256 fingerprint of its leaf certificate.
5. Configure the reciprocal peer entry on the other node. The configured host must match a DNS name or IP SAN in the remote certificate.
6. Set `network.bind-address` to the intended local interface, configure the same listening port on both nodes as appropriate, and restrict firewall access to known regional nodes.
7. Only then set `network.enabled: true` on both nodes and restart.

Each pair uses the lexicographically lower server ID as the outbound dialer. Both nodes must authorize one another by ID and certificate fingerprint. Invalid certificates, unknown IDs, handshake failures, and excess connections are rejected. If the configuration or TLS stores are invalid, GlobalSync disables itself rather than starting with weaker security.

**Do not expose this as a production replication network.** No outbound journal delivery or remote world changes are implemented. Remote commands, file access, plugin loading, and remote configuration changes are not part of the protocol. Keep networking disabled outside controlled connection tests.

The local outgoing event database is stored at `plugins/GlobalSync/journal/events.db`; the durable inbound inbox is `plugins/GlobalSync/journal/received.db`. Back them up with the server stopped or by using a SQLite-aware backup method.

Use `/globalsync status` to inspect the current node, capture state, journal offset, and authenticated peer count.

See [the architecture notes](docs/ARCHITECTURE.md), [the protocol notes](docs/PROTOCOL.md), and [the security policy](SECURITY.md) for implementation boundaries and operational requirements.
