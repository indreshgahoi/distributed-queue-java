# Distributed Queue in Java

A distributed message queue built incrementally from first
principles to explore delivery semantics, ownership, leases,
concurrency, durability, crash recovery, and replication.

## Why This Project

The goal is not to build another production message broker.

The goal is to understand which mechanisms are required
to provide specific queue guarantees and how those mechanisms
behave under failure.

## Current Scope

Latest release: v0.13.0 — snapshot-authorized WAL segment reclamation.

Current development: v0.14.0 — compaction-aware recovery authority. A queue
with reclaimed WAL history requires its authoritative snapshot and fails
closed rather than replaying an incomplete WAL suffix.

The implementation remains a single-node/local queue engine. Networking,
replication, partition ownership, and leader election are not yet included.

## Design Principles

- Define guarantees before implementation.
- Derive mechanisms from concrete failure scenarios.
- Prefer explicit state machines.
- Test failure behavior, not only happy paths.
- Add complexity only when a requirement justifies it.

## Roadmap

In-memory FIFO
→ acknowledgements
→ leases
→ retries
→ DLQ
→ concurrency
→ persistence
→ crash recovery
→ networking
→ replication
