# GlobalSync

GlobalSync is an experimental project exploring a shared Minecraft world across multiple geographically distributed Paper servers.

The intended experience is that players connect to a nearby regional server while still seeing and interacting with players in other regions. Player inventories and player sessions remain independent. The long-term design includes global chat, cross-region PvP, synchronized knockback, and replicated world state.

## Current status

**This repository contains the initial 26.3 plugin foundation and the first durable replication primitive. It is not yet a working multi-region world.**

Implemented so far:

- Paper 26.3 plugin entry point and strict Minecraft version guard.
- Unique regional node identity and shared-world configuration.
- A versioned replication event envelope.
- Local sequence generation restored from the durable journal after restart.
- SQLite append-only event journal using WAL mode and full synchronous commits.
- Idempotent retry handling and conflict detection for reused event IDs or node sequences.
- Bounded journal replay by local offset.
- Asynchronous journal append API so callers do not perform disk I/O on the Paper main thread.
- A status command and Maven/GitHub Actions verification workflow.

Not implemented yet:

- Peer discovery, authenticated network transport, or remote event delivery.
- Bukkit world-event capture and application of incoming events.
- Remote player entity spawning and movement replication.
- Chunk snapshots, block/container/redstone replication, or entity simulation.
- Global chat, cross-region damage, PvP, or KnockbackSync integration.
- Global commit ordering, consensus, conflict resolution, or replica repair.

The journal is a durable local history for one node. Its offset is local to that database and must not be treated as a global sequence or proof that another region has received an event.

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
6. Restart the server.

The local event database is stored at `plugins/GlobalSync/journal/events.db`. Back it up with the server stopped or by using a SQLite-aware backup method.

Use `/globalsync status` to inspect the current node and journal status.

## Configuration

`shared-worlds` lists world names intended for future replication. The network section is reserved for the transport milestone and is disabled by default. Do not expose an unauthenticated listener to the public internet.

See [the architecture notes](docs/ARCHITECTURE.md) for consistency goals and [the protocol notes](docs/PROTOCOL.md) for the event contract.

## Development principles

- Prefer Paper API over version-specific server internals.
- Keep transport, replication rules, and Bukkit integration separated.
- Never claim a local event is globally committed until the replication protocol can prove it.
- Make retries idempotent and recovery explicit.
- Keep the server responsive: network I/O and journal writes must not block the main thread.
