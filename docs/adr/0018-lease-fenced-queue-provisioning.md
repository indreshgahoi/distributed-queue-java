# ADR 0018: Lease-Fenced Queue Provisioning

## Status

Accepted for v0.19.0.

## Context

Queue creation durably records a `PROVISIONING` descriptor in PostgreSQL, but
v0.18.0 has no actor that creates the corresponding lineage-bound data-plane
storage. PostgreSQL and a node filesystem cannot commit atomically. A worker
may also crash, pause past its lease, or lose the response after completing a
metadata transition.

A process-local mutex cannot coordinate multiple future queue nodes. A lease
alone is also insufficient: expiration permits takeover but cannot stop the
old worker from resuming and publishing stale results.

## Decision

Add a separately deployable queue-node module with a scheduled reconciler.
The metadata service grants provisioning claims from PostgreSQL using row
locking with skip-locked selection. A claim contains the queue lineage,
worker ID, expiration time, and a monotonically increasing fencing token.

The worker creates deterministic storage for partition zero and opens the
segmented WAL with the exact queue/generation/partition lineage. It then asks
the metadata service to complete the claim. Completion and failure lock the
descriptor and claim, verify the complete claim identity and unexpired lease,
and conditionally transition the lifecycle state. A retry of an already
committed terminal transition by the same claim is idempotent.

The claim authorizes provisioning work only. It does not grant message-serving
ownership, placement, or leadership.

The protocol is illustrated in the
[provisioning sequence](../diagrams/provisioning-claim-sequence.md) and
[provisioning decision flow](../diagrams/provisioning-claim-flow.md).

## Consequences

### Positive

- a crashed worker's claim can be taken over after expiration;
- stale workers are fenced from publishing lifecycle results;
- repeated provisioning converges on the same lineage-bound storage;
- a queue becomes `ACTIVE` only after storage initialization succeeds;
- control-plane reconciliation is isolated from the local queue engine.

### Negative

- provisioning is asynchronous and temporarily exposes `PROVISIONING`;
- provisioning must finish within the fixed lease because renewal is absent;
- filesystem effects from a fenced worker cannot be rolled back, so storage
  operations must remain idempotent and lineage-validated;
- the trusted internal HTTP API is not yet authenticated;
- failed descriptors require explicit future retry policy.

## Alternatives Considered

### Mark the queue active during creation

Rejected because a PostgreSQL transaction cannot prove that remote filesystem
storage exists and is recoverable.

### Use a lease without a fencing token

Rejected because the expired worker may resume after another worker takes over
and incorrectly publish a stale result.

### Hold a database transaction while creating storage

Rejected because filesystem work is not transactional with PostgreSQL and a
long-lived transaction would retain locks without eliminating partial failure.

### Introduce placement and leader election simultaneously

Rejected because provisioning authority is narrower than runtime data-plane
ownership. Combining them would obscure their separate invariants and failure
modes.

## Revisit When

- provisioning can exceed one claim duration and requires renewal;
- nodes register durably and placement becomes metadata authority;
- runtime partition ownership needs its own epoch and fencing protocol;
- internal APIs require authentication and authorization;
- failed provisioning receives automated retry and cleanup policy.
