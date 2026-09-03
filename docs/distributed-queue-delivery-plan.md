# Distributed Queue Delivery Plan

This plan converts the target architecture into reviewable milestones. Each
phase follows: semantics, invariants, failure scenarios, tests, implementation,
regression tests, documentation, commit, and release.

## Phase 0 — Close v0.27 Honestly

Goal: finish bounded transport without implying automatic replication.

- verify HTTP client timeout and batch-bound tests;
- test partial-prefix failure and unchanged retry;
- document that replica membership and scheduling do not exist;
- do not call follower data committed.

## Phase 1 — v0.27.1: Replication Performance Baseline

Limitation solved: the design does not know whether record-by-record force,
encoding, locks, HTTP, snapshot I/O, or partition density is the actual
bottleneck.

- benchmark forced leader WAL append by payload and concurrency;
- benchmark the current follower batch, which forces every record;
- prototype batch write plus one force without changing production semantics;
- measure batch sizes 1, 8, 32, 128, and 256;
- measure segment rotation and concurrent snapshot interference;
- measure HTTP serialization separately from follower disk durability;
- measure many idle and active partitions on one node;
- check in raw JMH JSON, environment metadata, and interpretation.

Exit criterion: v0.28 storage APIs are selected from measured evidence, with
p50/p95/p99/p99.9/max and throughput recorded. Full matrix:
[Group Commit and Durability](architecture/appendix-c-group-commit-and-durability.md).

## Phase 2 — v0.28: Durable Logical Replicated Log

Limitation solved: record counting cannot preserve logical sequence across WAL
reclamation and does not store the leader term with each entry.

- encode `logIndex` and `logTerm` atomically with each replicated WAL entry;
- expose bounded reads by logical index;
- persist replica hard state: current term, vote, and commit index;
- extend snapshots with last included index and term;
- define upgrade policy explicitly; no migration is required for this learning
  repository unless chosen before implementation;
- prohibit compaction from destroying required logical history.

Exit criterion: restart and snapshot plus WAL recovery reproduce
`lastApplied <= commitIndex <= lastLogIndex` without counting reclaimed records.

## Phase 3 — v0.29: Replica Membership and Placement

Limitation solved: the system cannot identify which nodes should store a
partition, so v0.27 cannot schedule replication safely.

- add queue-generation replication factor, default three;
- model voter versus learner replicas;
- place replicas across distinct live nodes and, later, failure domains;
- prevent one node from hosting two replicas of the same partition;
- expose desired membership to queue nodes;
- keep membership immutable after bootstrap in this phase.

Exit criterion: every partition has one inspectable desired replica set and
nodes materialize only assigned replicas.

## Phase 4 — v0.30: Automatic Catch-Up and Learner Bootstrap

Limitation solved: follower transport exists but no controller drives it.

- start bounded per-follower replication workers on the current leader;
- maintain `nextIndex` and `matchIndex` per follower;
- retry with bounded exponential backoff and jitter;
- expose lag, last success, and failure metrics;
- install a snapshot when requested history has been reclaimed;
- rate-limit recovery across many partitions on one node.

Exit criterion: an assigned learner automatically reaches the leader's log end
without blocking local queue mutation locks.

Benchmark gate: demonstrate bounded threads/connections, fair progress across
partitions, backoff under unavailable followers, and controlled memory/WAL growth
under a slow follower.

## Phase 5 — v0.31: Majority Commit and Committed-Only Apply

Limitation solved: a local append is acknowledged even though loss of that node
can lose the operation.

- compute majority from voting membership;
- advance monotonic `commitIndex` from follower match indexes;
- propagate `leaderCommit` in append and heartbeat requests;
- apply queue transitions only through committed index;
- acknowledge publish, lease, ACK, NACK, expiry, and DLQ transitions only after
  majority commit;
- stop mutations immediately after losing majority authority.

Exit criterion: acknowledged mutations survive any one replica failure with
replication factor three.

Benchmark gate: decompose end-to-end p99 into leader queueing, leader force,
network, follower force, commit propagation, and state-machine apply.

## Phase 6 — v0.32: Node-Coordinated Leader Election

Limitation solved: leader failure still requires external/manual authority.

- implement follower, candidate, and leader roles per partition;
- add randomized election timeouts and heartbeats;
- durably persist term and vote before responding;
- require majority vote and Raft-style log freshness;
- fence old leaders using terms;
- report elected leadership to metadata for discovery;
- keep PostgreSQL outside the voting and message commit path.

Exit criterion: a three-replica partition elects a safe leader after one node
failure and refuses two simultaneous majority-capable leaders.

## Phase 7 — v0.33: Safe Promotion and Replica Repair

Limitation solved: returning nodes and divergent uncommitted suffixes cannot yet
be reconciled automatically.

- truncate only uncommitted conflicting suffixes under current leader authority;
- require committed state application before serving traffic;
- promote caught-up learners through a safe membership transition;
- replace failed replicas without reducing the old majority prematurely;
- test partitions, repeated crashes, and recovery during snapshot transfer.

Exit criterion: permanent node loss is repaired while preserving every
majority-acknowledged transition.

Benchmark gate: measure foreground p99 while installing snapshots and while
recovering many replicas after one node failure.

## Phase 8 — Multi-Partition Queue Semantics

Limitation solved: each queue still has only partition zero.

- make partition count immutable per generation;
- add stable keyed publication routing;
- add bounded rotating receive probes;
- carry authenticated partition identity in receipt handles;
- route ACK and NACK to the current leader of the originating partition;
- document partition-local ordering and approximate empty receive.

Exit criterion: one customer queue scales across partitions without claiming a
global order.

Benchmark gate: measure keyed distribution, hot partitions, bounded receive
probe hit rate, and node resource use as partition count increases.

## Issue-Ready Backlog

Create one GitHub milestone per phase and one issue per bullet group below.

### v0.27.1

1. Capture forced-WAL baseline by payload and concurrency.
2. Benchmark current per-record-force follower batches.
3. Prototype and benchmark one-force batch append.
4. Measure segment rotation and snapshot interference.
5. Measure HTTP-only and HTTP-plus-disk follower paths.
6. Measure idle and active partition density.
7. Check in raw results, environment manifest, and engineering interpretation.

### v0.28

1. Specify replicated log frame and hard-state formats.
2. Implement logical-index append and bounded read contracts.
3. Persist and recover term, vote, commit index, and applied index.
4. Extend snapshot authority with last included index and term.
5. Integrate replication-safe WAL reclamation.
6. Add crash matrix and property/state-machine tests.

### v0.29

1. Add replication factor to queue-generation metadata.
2. Model voter and learner replica membership.
3. Implement failure-domain-aware initial replica placement.
4. Add node replica reconciliation and local replica catalog.
5. Expose membership inspection APIs and operational logs.

### v0.30

1. Implement per-follower next-index and match-index tracking.
2. Add bounded replication scheduler with backoff and jitter.
3. Implement snapshot installation protocol.
4. Add recovery bandwidth and concurrency limits.
5. Add replica lag metrics and fault-injection integration tests.

### v0.31

1. Define majority commit semantics for every queue transition.
2. Implement leader commit-index advancement.
3. Propagate leader commit and implement follower committed-only apply.
4. Gate client responses on majority commit.
5. Test leader crash before and after quorum acknowledgement.

### v0.32

1. Specify election timers, terms, votes, and role transitions.
2. Implement durable vote granting and log freshness checks.
3. Implement heartbeats and majority-loss step-down.
4. Integrate gateway discovery with reported leadership.
5. Build deterministic partition and split-vote tests.

### v0.33

1. Implement conflict discovery and uncommitted suffix truncation.
2. Implement learner promotion and safe membership transition.
3. Implement dead-replica replacement workflow.
4. Test returning stale nodes and repeated failures during repair.

### Multi-partition

1. Define immutable queue-generation partition configuration.
2. Implement stable keyed publish routing.
3. Implement bounded fair receive probing.
4. Add signed partition-aware receipt handles.
5. Add partition-local ordering and routing failure tests.

## GitHub Publishing

Remote issue creation requires an authenticated GitHub CLI session:

```bash
gh auth login -h github.com
```

After authentication, publish milestones and issues from this plan. Keep issue
titles problem-oriented and include invariant, failure tests, non-goals, and exit
criteria in every issue body.
