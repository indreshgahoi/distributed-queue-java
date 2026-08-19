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

v0.1 — Single-process in-memory FIFO queue.

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