# Architecture

## Target topology

Each region runs a Paper 26.3 server and keeps a complete local copy of the shared world. Players connect to the region with the best route from their location. A replication layer distributes committed changes between replicas.

A complete copy on every node does not mean every node should independently simulate every object. That would cause divergent mob AI, random ticks, fluids, redstone, and entity movement.

## Consistency model

The intended model is an ordered event stream per shared world, with explicit ownership/authority for operations that cannot safely execute concurrently.

A future implementation must define:

1. A durable event log and monotonic committed offsets.
2. An authority/lease model for entity simulation and conflicting world mutations.
3. Chunk versions and snapshot boundaries.
4. Duplicate suppression that survives process restarts.
5. Backpressure and bounded queues during regional outages.
6. Recovery from a known committed offset, with snapshot fallback.
7. A clear policy for partitions: pause or reject writes that cannot be safely ordered instead of allowing split-brain state.

A local atomic counter is not a global sequence. The SequenceClock in this milestone only orders events emitted inside one process.

## Threading

Bukkit/Paper world access must happen on the server's appropriate scheduler/thread. Network reads and writes must run off the main thread. Incoming messages should be validated off-thread, then marshalled onto the correct server thread before touching world state.

## Replication layers

- **Presence:** online node registry and remote player metadata.
- **Player view:** remote player spawn, movement interpolation, equipment, metadata, and removal.
- **World events:** block and block-entity changes, containers, explosions, fluids, redstone, and scheduled updates.
- **Chunk state:** versioned snapshots and incremental updates.
- **Entities:** single-writer simulation authority with replicated state.
- **Combat:** one authoritative hit/damage decision, with the outcome and velocity replicated to both regions.
- **World metadata:** time, weather, gamerules, and world border.

## Failure behavior

A regional partition must not silently permit conflicting writes to the same authoritative state. Nodes should track their last committed offset, detect gaps, and resynchronize from the durable log or a snapshot. A disconnected region may continue read-only, or accept only operations explicitly safe to queue, until it can rejoin without creating split-brain state.

## Plugin boundary

The current plugin is intentionally a thin Paper integration. Protocol types and replication primitives should remain independent of Bukkit classes so they can be tested without starting a Minecraft server. If full-fidelity capture of internal simulation changes cannot be achieved through stable Paper APIs, a narrowly scoped server integration layer may be required.
