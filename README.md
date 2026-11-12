# GlobalSync

GlobalSync is an experimental project exploring a shared Minecraft world across multiple geographically distributed Paper servers.

The intended experience is that players connect to a nearby regional server while still seeing and interacting with players in other regions. Player inventories and player sessions remain independent. The long-term design includes global chat, cross-region PvP, synchronized knockback, and replicated world state.

## Current status

**This repository contains a 26.3 plugin foundation, a durable local event journal, initial player block-edit capture, and reusable secure-transport primitives. It is not yet a working multi-region world.**

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
- TLS 1.3 context loading from PKCS12 identity and trust stores.
- Mutual-TLS server socket configuration and explicit SHA-256 peer certificate fingerprint verification helpers.
- Bounded length-prefixed message framing (1 MiB default maximum).
- A status command and Maven/GitHub Actions verification workflow.

Not implemented yet:

- A live peer listener, outbound peer connections, or peer discovery.
- Loading transport settings or TLS credentials from plugin configuration.
- Remote event delivery, acknowledgements, retries, or durable receiver-side deduplication.
- Applying incoming block changes on another server.
- Rollback or compensation if a remote region disagrees with a local edit.
- Capture of explosions, pistons, fluids, redstone, growth, containers, block entities, or other non-player world changes.
- Chunk snapshots, entity simulation, and remote player rendering.
- Global chat, cross-region damage, PvP, or KnockbackSync integration.
- Global commit ordering, consensus, conflict resolution, or replica repair.

Player block edits are currently journaled locally only. They are not transmitted, and a local journal offset is not a global commit index.

## Requirements

- Minecraft **26.3 only**
- Paper **26.3**
- Java **25**
- Maven 3.9+

Paper API is compiled as a provided dependency. Paper's plugin library loader downloads SQLite JDBC and Gson at runtime.

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
7. Keep `network.enabled: false`; this build does not open peer sockets.
8. Restart the server.

The local event database is stored at `plugins/GlobalSync/journal/events.db`. Back it up with the server stopped or by using a SQLite-aware backup method.

Use `/globalsync status` to inspect the current node, capture state, and journal offset.

See [the architecture notes](docs/ARCHITECTURE.md) for consistency goals and [the protocol notes](docs/PROTOCOL.md) for the event and transport contract.
