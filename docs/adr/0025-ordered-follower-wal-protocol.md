# ADR 0025: Establish an Ordered Follower WAL Protocol

## Status

Accepted for v0.26.0.

## Context

The system has durable, lineage-bound local storage but no distributed storage
primitive. Sending raw `WalRecord` values to another node would not identify
order, distinguish retry from conflict, or prevent an obsolete leader from
continuing to mutate a follower.

Physical `WalPosition` identifies bytes for local snapshot recovery. It is not
a stable logical replication index, particularly after segment reclamation.

## Decision

Introduce `ReplicatedWalEntry` with storage lineage, leader epoch, global
sequence, and the exact `WalRecord`. `OrderedFollowerReplicaLog` enforces:

1. exact lineage equality;
2. rejection of epochs below the highest durably observed epoch;
3. append only at `lastSequence + 1`;
4. idempotent success for an identical previously stored sequence;
5. conflict for different content at a stored sequence.

The highest leader epoch is written to a versioned, lineage-bound, CRC32C
protected candidate, forced, atomically promoted, and followed by parent
directory force before sequence validation and WAL append. Epoch is authority;
sequence is log consistency. A higher epoch therefore fences an older leader
even if the supplied entry later fails gap or conflict validation. A storage
failure poisons the follower writer.

Until logical sequence is included in replication-aware checkpoints, the
follower requires complete WAL history and fails closed on a reclaimed suffix.

## Consequences

- stale-leader fencing and exact retry behavior survive restart;
- epoch advancement can safely exist without a record after a crash;
- validation is deterministic and independent of networking or metadata;
- initialization is O(retained records) and complete history is temporarily
  required;
- no claim is made about quorum commit, elections, promotion, or availability.
