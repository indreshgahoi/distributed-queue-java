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

Latest release: v0.14.0 — compaction-aware recovery authority.

Current development: v0.15.0 — durable filesystem authority transitions.
Snapshot promotion, WAL segment publication, and segment reclamation include
the parent-directory durability boundary rather than relying on atomic rename
or file forcing alone.

The implementation remains a single-node/local queue engine. Networking,
replication, partition ownership, and leader election are not yet included.

## Design Principles

- Define guarantees before implementation.
- Derive mechanisms from concrete failure scenarios.
- Prefer explicit state machines.
- Test failure behavior, not only happy paths.
- Add complexity only when a requirement justifies it.

## Roadmap

The roadmap is organized around correctness problems, not feature parity with
existing brokers. Each milestone must identify a concrete limitation, define
its invariants and failure semantics, and preserve a coherent path toward a
distributed system.

### Completed foundations

1. **Queue state machine** — FIFO publication, explicit acknowledgement,
   receipt-handle ownership, finite leases, redelivery, bounded retries,
   delayed NACK, and dead-letter transitions.
2. **Single-process concurrency** — coarse-grained locking establishes an
   understandable linearization point for compound queue transitions.
3. **Durable transitions** — WAL-first mutation ordering, failed-writer
   poisoning, framed records, versioned headers, CRC32C integrity, and
   recoverable active-tail truncation.
4. **Durable delivery ownership** — active leases, attempts, receipt handles,
   expiry, ACK, NACK, and DLQ decisions survive restart.
5. **Bounded recovery history** — snapshots, `WalPosition`, snapshot plus WAL
   suffix recovery, crash-safe snapshot replacement, segmented WAL rotation,
   and snapshot-authorized whole-segment reclamation.
6. **Compaction-aware recovery** — startup fails closed when reclaimed history
   makes the authoritative snapshot mandatory, and snapshot authority remains
   monotonic across restart.

### Current milestone

**v0.15.0 — Durable filesystem authority transitions**

Close the gap between durable file contents and durable filesystem names by
forcing parent directories after snapshot promotion, WAL segment publication,
and segment deletion.

### Next decision area

After v0.15.0, the repository will be reviewed again before selecting a
milestone. Likely candidates are:

- **Automated storage lifecycle** — prevent operationally unbounded WAL growth
  when callers do not manually schedule snapshots and reclamation.
- **Storage lineage identity** — detect snapshots and WAL segments copied from
  different queue generations instead of accepting structurally compatible
  but unrelated recovery artifacts.
- **Admission control and backpressure** — prevent unbounded heap, queue-depth,
  and disk consumption under sustained producer load.
- **Producer idempotency** — resolve duplicate publication when a producer
  retries after an ambiguous response.

Selection will be based on correctness value, architectural dependency,
failure exposure, operational need, and distributed-systems learning value.

### Deliberately later

Networking, partitioning, ownership transfer, leader election, replication,
and quorum durability come only after the local durability and storage
lifecycle contracts are explicit and tested. These phases will introduce the
distributed concerns of fencing, partial failure, split brain, replica lag,
and recovery-source authority rather than merely adding remote APIs.
