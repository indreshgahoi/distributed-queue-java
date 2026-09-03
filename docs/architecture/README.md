# Distributed Queue Architecture Handbook

This directory is the living design reference for the distributed queue. Read
the documents in order when learning the system or reviewing a milestone.

1. [Partition model](appendix-a-partition-model.md) — how tenants, queues,
   partitions, replicas, and nodes relate.
2. [Replication protocol](appendix-b-replication-protocol.md) — leader/follower
   flow, ordering, catch-up, commit, and failure behavior.
3. [Group commit and durability](appendix-c-group-commit-and-durability.md) —
   how batching reduces force cost without weakening acknowledgement semantics.
4. [Metadata model](appendix-d-metadata-model.md) — proposed control-plane
   tables, ownership, constraints, and what must not live in PostgreSQL.
5. [Guarantee matrix](appendix-e-guarantee-matrix.md) — guarantees by operation,
   failure, and implementation phase.

The concise target is in
[Distributed Queue Target Architecture](../distributed-queue-target-architecture.md),
and implementation order is in the
[Distributed Queue Delivery Plan](../distributed-queue-delivery-plan.md).

## Living-document rules

- Update these documents before implementing a changed invariant.
- Label statements as **current**, **target**, or **deferred**.
- Link accepted choices to an ADR once implementation begins.
- Keep physical storage facts separate from control-plane observations.
- Record benchmark environment and raw results; do not publish only averages.
- A diagram is explanatory, not authoritative when it conflicts with semantics
  or executable tests.
