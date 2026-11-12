# GlobalSync

GlobalSync is an experimental project exploring a shared Minecraft world across multiple geographically distributed Paper servers.

The intended experience is that players connect to a nearby regional server while still seeing and interacting with players in other regions. Player inventories and player sessions remain independent. The long-term design includes global chat, cross-region PvP, synchronized knockback, and replicated world state.

## Current status

**This repository currently contains the initial 26.3 plugin foundation, not a working multi-region world.**

Implemented in this milestone:

- Paper 26.3 plugin entry point and strict Minecraft version guard.
- Unique regional node identity and shared-world configuration.
- A versioned replication event envelope.
- Local sequence generation and bounded in-process duplicate detection.
- A status command.
- Maven build and GitHub Actions verification workflow.

Not implemented yet:

- Peer discovery, authenticated network transport, or durable event delivery.
- Remote player entity spawning and movement replication.
- Chunk snapshots, block/container/redstone replication, or entity simulation.
- Global chat, cross-region damage, PvP, or KnockbackSync integration.
- Crash-safe consensus, conflict resolution, or replica repair.

Those are deliberate follow-up milestones. The local sequence clock and duplicate filter are building blocks only; they do not provide global ordering, durable idempotency, or distributed consistency.

## Requirements

- Minecraft **26.3 only**
- Paper **26.3**
- Java **25**
- Maven 3.9+

Paper API is compiled as a provided dependency and is not bundled into the plugin.

## Build

```bash
mvn --batch-mode verify
```

The resulting plugin jar is in `target/`. GitHub Actions runs the same verification command on pushes and pull requests.

## Install

1. Build the project with Java 25.
2. Copy `target/globalsync-0.1.0-SNAPSHOT.jar` into the Paper server's `plugins` directory.
3. Start the server once to generate `plugins/GlobalSync/config.yml`.
4. Set a unique `server-id` for each region.
5. Restart the server.

Use `/globalsync status` to inspect the current node status.

## Configuration

`shared-worlds` lists world names intended for future replication. The network section is reserved for the transport milestone and is disabled by default. Do not expose an unauthenticated listener to the public internet.

See [the architecture notes](docs/ARCHITECTURE.md) for consistency goals and [the protocol notes](docs/PROTOCOL.md) for the event contract.

## Development principles

- Prefer Paper API over version-specific server internals.
- Keep transport, replication rules, and Bukkit integration separated.
- Never claim a local event is globally committed until the replication protocol can prove it.
- Make retries idempotent and recovery explicit.
- Keep the server responsive: network I/O must not block the main thread.
