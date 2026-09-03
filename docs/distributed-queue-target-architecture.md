# Distributed Queue Target Architecture

## Purpose

This document is the architectural map for evolving the current durable local
queue into a multi-tenant, partitioned, replicated queue. It separates the
target model from the smaller milestones used to reach it. A milestone must not
claim a target guarantee until its invariants and failure tests exist.

Detailed living appendices are indexed in the
[Architecture Handbook](architecture/README.md).

## Decisions at a Glance

| Question | Target decision |
|---|---|
| Replication factor | Queue configuration; default `3` |
| Ordering | Per partition, never queue-wide across partitions |
| Leader election | Node-coordinated quorum election; PostgreSQL supplies desired membership and discovery only |
| Publish acknowledgement | Durable majority quorum |
| Log sequence assignment | Inside the partition leader's serialized WAL append |
| Commit index | Monotonic per partition; persisted in replica hard state and propagated as `leaderCommit` |
| Follower state replay | Yes, concurrently with replication, but only through committed entries |
| Promotion eligibility | Voting member, current lineage, live, quorum-connected, and log at least as up-to-date as voters require |

These are target decisions. The current implementation does not yet provide
quorum publication, election, promotion, or committed-only follower replay.

## 1. Resource and Failure Hierarchy

```text
Tenant
└── Queue
    └── Generation
        ├── Partition 0
        │   ├── Replica on node A (leader)
        │   ├── Replica on node B (follower)
        │   └── Replica on node C (follower)
        └── Partition N
            ├── Replica on node B (leader)
            ├── Replica on node C (follower)
            └── Replica on node D (follower)
```

The partition is the unit of ordering, replication, leadership, commit,
snapshotting, recovery, and failover. A node is only a temporary host for many
independent replicas from many tenants. Node failure creates multiple
independent partition recovery operations, not one node-wide queue recovery.

Durable lineage remains:

```text
queueId + generationId + partitionId
```

Physical replica identity adds `replicaId` and `nodeId`; neither changes the
logical partition lineage.

## 2. Replication Factor

`replicationFactor` belongs to immutable queue-generation configuration and
defaults to three.

For the first replicated release, support:

- `1` for explicitly non-replicated development queues;
- `3` as the default production-learning configuration;
- optionally `5` later for tolerating two failures at greater cost.

Avoid a default of two. A two-replica majority is two, so either replica failure
removes write availability. Three replicas require two acknowledgements and can
tolerate one replica failure without sacrificing majority safety.

Changing replication factor is a membership transition, not an ordinary
configuration update. It must use joint consensus or another explicitly safe
transition protocol later. Until then it is immutable for a generation.

## 3. Control Plane and Consensus Boundary

PostgreSQL should own:

- tenant and queue definitions;
- partition count and desired replication factor;
- desired replica placement;
- node registration and administrative draining;
- observable leader and health information reported by replicas;
- gateway discovery hints.

PostgreSQL must not own:

- allocation of message log indexes;
- the committed message prefix;
- votes in a partition election;
- permission for a leader to acknowledge a publish;
- authoritative recovery choice between divergent logs.

The long-term leadership protocol is node-coordinated per partition, following
Raft-style term and voting rules. A candidate becomes leader only after receiving
votes from a majority of the current voting replica set. Each voter durably
persists its term and vote before responding.

This choice means PostgreSQL failure prevents queue creation, placement changes,
and possibly fresh gateway discovery, but an established replica group can
continue replication and election as long as it retains a majority. PostgreSQL
itself should still be deployed with database high availability; removing it
from consensus does not make an unreplicated database operationally safe.

### Bootstrap stage

Before node election exists, PostgreSQL may assign the initial leader and epoch
for a newly empty partition. Automatic failover must remain disabled in that
stage. A database lease alone cannot prove which WAL prefix is safe to promote.

## 4. Partition Log and Sequence Assignment

The leader serializes concurrent mutations through one partition append path:

```text
acquire partition append permit
    ↓
verify current leader term and role
    ↓
nextIndex = lastLogIndex + 1
    ↓
encode {term, index, WalRecord} in one WAL frame
    ↓
append and force frame
    ↓
publish entry to follower replication workers
```

The index is chosen during the local WAL append operation, under the same
serialization boundary. It must not be handed to a request before the leader
knows which log entry will occupy it. A failed or indeterminate append poisons
the writer; the process must recover that storage before issuing another index.

The replicated frame ultimately needs both `term` and `index`. Counting records
from complete WAL history, as v0.26 does, is a temporary bridge and is not safe
after reclamation.

## 5. Majority Publish Acknowledgement

For replication factor three, a publish becomes committed after the exact entry
is durable on any two voting replicas, normally the leader and one follower.

```text
RF = 3
majority = floor(3 / 2) + 1 = 2
```

The producer receives success only after:

1. the entry is durably appended locally;
2. matching `(term, index, content)` is durably acknowledged by a majority;
3. the leader advances its commit index to include the entry;
4. the queue state machine applies the committed publish sufficiently to return
   its stable message identity.

A slow third replica does not block success. If the leader loses the majority,
it must reject or stop new mutations even when its local disk is healthy.

Receive, ACK, NACK, lease-start, lease-expiry, and DLQ transitions are also
mutating log operations. They eventually need the same commit rule; applying
majority only to `PUBLISH` would leave delivery ownership inconsistent after
failover.

## 6. Commit Index

`commitIndex` is the highest contiguous log index known to be durably replicated
on a majority and therefore safe for the state machine to apply.

Example:

```text
Leader log:       1 2 3 4 5 6 7
Follower B:       1 2 3 4 5 6
Follower C:       1 2 3 4
Majority match:   1 2 3 4 5 6
commitIndex:      6
```

The leader tracks each follower's `matchIndex`. It advances `commitIndex` to the
largest index stored by a majority, subject to the election protocol's current-
term commit rule. Commit is always a contiguous prefix; index seven cannot be
committed while index six is missing.

### Persistence

Each replica maintains durable hard state:

```text
currentTerm
votedFor
commitIndex
```

Term and vote must be forced before granting authority. Persisting commit index
allows a restart to distinguish committed from merely appended records without
temporarily applying too much state. Publication should use the same
candidate-force-atomic-promote-directory-force pattern already used for storage
authority files.

The snapshot carries:

```text
lastIncludedIndex
lastIncludedTerm
state machine image through that index
```

WAL segments can be reclaimed only when the snapshot covers them and replica
catch-up policy no longer requires them.

### Propagation

Every leader replication request includes `leaderCommit`. After appending the
entries, a follower sets:

```text
followerCommitIndex = min(leaderCommit, followerLastLogIndex)
```

It persists the monotonic commit index and then makes newly committed entries
available to its apply loop. A heartbeat with no entries can propagate a newer
commit index.

If a leader crashes after a majority stores an entry but before followers hear
the new commit index, a subsequent valid leader can rediscover and commit that
entry through the election/log-matching protocol. Therefore commit is a quorum
fact, not merely a flag in the old leader's memory.

## 7. Follower Replay During Replication

Followers should replay queue state while continuing to receive replication.
They must apply only through `commitIndex`.

Use two ordered positions per replica:

```text
lastLogIndex     highest durably appended entry
commitIndex      highest quorum-committed entry
lastApplied      highest entry applied to queue state
```

The relationship is always:

```text
lastApplied <= commitIndex <= lastLogIndex
```

A per-partition append path writes incoming entries. A separate single-threaded
per-partition apply loop advances state in index order. Snapshot creation takes a
consistent state-machine boundary without blocking network replication longer
than necessary.

Followers do not serve customer receive, ACK, NACK, or publish requests. Replay
keeps them warm so promotion does not require rebuilding the entire queue state
after leader failure.

## 8. Promotion Eligibility

Possible policies include:

1. **Coordinator chooses the highest reported index.** Simple, but unsafe when
   reports are stale or the chosen log contains uncommitted divergent entries.
2. **Require an exact copy of the old leader.** Safe-looking but unnecessarily
   unavailable; the old leader may be permanently lost and its uncommitted tail
   is not authoritative.
3. **Quorum election with log freshness checks.** A majority votes only for a
   candidate whose `(lastLogTerm, lastLogIndex)` is at least as up-to-date as
   the voter's log. This is the recommended target.

A follower is eligible to campaign only when it:

- belongs to the current voting membership, not a learner;
- has exact queue generation and partition lineage;
- is not administratively draining or storage-poisoned;
- has durably persisted its current term and vote;
- can contact a majority;
- passes the log up-to-date voting rule;
- has a supported snapshot/WAL format and valid storage;
- has applied at least the committed prefix before serving traffic.

Election chooses an authoritative log; it does not choose merely the node with
the lowest latency. Uncommitted divergent suffixes are reconciled only after a
new leader is established and must never be exposed to consumers beforehand.

## 9. Multi-Queue Node Failure

If node A hosts 100 replicas, its failure produces up to 100 independent
partition events:

```text
for each affected partition:
    if A was follower:
        leader continues with majority if available
        placement controller schedules a replacement learner
    if A was leader:
        remaining voters independently run election
        gateway discovers the new leader
        replacement learner is added later
    if no majority remains:
        partition rejects mutations and becomes unavailable
```

Healthy partitions on other nodes continue. Recovery work must be bounded and
rate-limited so restoring many replicas does not exhaust disk or network and
harm serving partitions.

## 10. Multi-Partition Data Plane

Publish selects exactly one partition. A stable hash of `messageGroupId` or
partition key preserves group locality. Unkeyed publishing may use a generated
message identity hash. The selected partition cannot change merely because its
leader is unavailable.

Receive probes a bounded rotating subset of partition leaders and may return
empty even if an unprobed partition has a ready message. ACK and NACK route using
authenticated partition identity carried in an opaque receipt-handle envelope.

There is no total queue-wide ordering across partitions. Partition count should
remain immutable for a queue generation until safe repartitioning is designed.

## 11. Availability Boundaries

| Failure | Expected target behavior |
|---|---|
| One follower unavailable with RF=3 | Leader continues majority commits |
| Leader unavailable, two replicas connected | Elect a new leader and resume |
| Only one replica reachable with RF=3 | Reject mutations; never claim quorum durability |
| PostgreSQL unavailable, stable group has majority | Existing group continues; topology changes stop |
| Gateway metadata lookup unavailable | Cached routes may work only with bounded staleness and node-side fencing |
| Follower far behind retained WAL | Install snapshot, then stream WAL suffix |
| Node returns with obsolete term | Reject and reconcile as follower |

## 12. Deliberately Deferred

- global ordering across partitions;
- cross-partition transactions;
- exactly-once end-to-end processing;
- automatic partition-count expansion;
- multi-region consensus;
- observer/witness replicas;
- dynamic membership before joint-consensus rules exist;
- serving reads or receives from followers.

## 13. Architecture Review Gate

Before implementing each phase, answer:

1. What durable authority changes?
2. What is the linearization or commit point?
3. What survives process, node, disk, and network failure?
4. Which stale actor is fenced, and by what durable value?
5. Can a partial operation be retried safely?
6. Which state is authoritative after histories disagree?
7. What new guarantee is made, and what remains explicitly unavailable?

The phased execution plan is maintained in
[distributed queue delivery plan](distributed-queue-delivery-plan.md).
