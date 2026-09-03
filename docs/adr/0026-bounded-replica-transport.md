# ADR 0026: Add Bounded Replica Transport and Catch-Up

## Status

Accepted for v0.27.0.

## Context

v0.26 defines correct follower acceptance but only through an in-process API.
Replication needs a node boundary, yet the current metadata model has one
placement and no authoritative replica set. Automatically selecting targets at
this stage would hide an undefined ownership decision inside a scheduler.

Network failure also makes a batch response ambiguous, and unbounded batches
could monopolize memory and follower storage I/O.

## Decision

Add an internal queue-node endpoint that accepts one lineage, leader epoch,
first sequence, and one to 256 consecutive `WalRecord` values with at most 1 MiB
of payload. Apply records sequentially through `OrderedFollowerReplicaLog` and
return durable progress plus new-versus-duplicate counts.

Add a follower HTTP client and `ReplicaCatchUpService`, which performs exactly
one bounded read and at most one remote call. Its source and client are ports;
membership and scheduling remain outside the service.

Follower storage opens lazily beneath the lineage storage path and is closed at
node shutdown. Existing WAL locking prevents a local primary and follower
handle from concurrently owning the same lineage.

## Consequences

- two processes can exercise the durable follower protocol over HTTP;
- unchanged retries recover from lost responses and partial batch completion;
- 409 responses distinguish consistency/authority rejection from storage
  failure;
- no replica is selected or called automatically yet;
- complete logical history remains required until replication-aware snapshot
  transfer and indexing are introduced.
