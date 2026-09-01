## v0.7 — Coarse-Grained Locking

### Decision
Use a single `ReentrantLock` to protect all queue state transitions.

### Why
It provides a simple correctness model for transitions spanning multiple
data structures.

### Trade-off
All state-changing operations are serialized through one lock.

This may limit concurrency under contention, but the performance impact
has not yet been measured.

### Deferred alternatives
- Fine-grained locking
- Concurrent data structures
- Lock striping

### Revisit when
Benchmarks show lock contention is a meaningful bottleneck.

## v0.8 — Force WAL on Every Durable Write

### Decision
Force each committed WAL record before reporting the operation successful.

### Why
This provides a simple durability contract for the first persistent version.

### Trade-off hypothesis
Per-operation forcing may increase latency and reduce throughput.

This has not yet been measured.

### Deferred alternatives
- Buffered WAL writes
- Group commit
- Periodic fsync
- Asynchronous durability

### Revisit when
Durability semantics are stable and benchmarks are available.

## v0.10.0 — CRC32C WAL Integrity

### Decision

Add CRC32C to every complete WAL frame.

### Gain

Recovery can detect structurally complete records whose payload bytes have
been corrupted.

### Cost

Each frame gains 4 bytes and requires checksum computation on write and
verification during recovery.

The performance impact has not yet been measured.

### Important distinction

Incomplete final frame:
recoverable torn-tail condition.

Complete frame with checksum mismatch:
corruption; fail recovery.

## v0.11.1 — Durable Delivery Leases

### Decision

Persist a `LEASE_STARTED` WAL record before exposing a delivery to a consumer.

A lease contains:

    messageId
    receiptHandle
    attempt
    leaseUntil

Active leases are reconstructed across queue restart.

### Benefit

The queue can preserve externally visible delivery ownership across restart.

Without durable lease creation:

    receive
        |
        v
    queue restart
        |
        v
    message becomes READY prematurely

With durable lease creation:

    receive
        |
        v
    persist lease
        |
        v
    queue restart
        |
        v
    restore IN_FLIGHT

This prevents restart itself from changing delivery semantics.

### Cost

`receive()` now includes durable storage work.

Every delivery attempt adds another WAL record.

This may:

- increase receive latency;
- increase WAL write volume;
- increase WAL growth rate;
- increase recovery work.

These effects have not yet been benchmarked.

### Alternative — Immediately Requeue After Restart

Gain:

- simple recovery;
- no need to persist delivery ownership.

Cost:

- restart becomes implicit lease cancellation;
- potential premature duplicate delivery;
- existing consumer ownership is lost.

Rejected.

### Alternative — Preserve Lease Time but Not Receipt Handle

Gain:

- prevents premature redelivery.

Cost:

- old consumer cannot ACK/NACK after queue restart;
- successfully processed messages may later be redelivered.

Rejected.

### Selected Trade-off

Preserve the complete lease.

This makes the semantic model stronger and more internally consistent at the
cost of additional durable writes on the receive path.

### Revisit When

Revisit this decision if measurements show that durable lease creation is a
meaningful throughput or latency bottleneck.

Any optimization must explicitly state whether it preserves the same durability
contract.

## v0.12.1 — Snapshot WAL Position: Segment ID + Offset

### Decision

Represent snapshot recovery position as:

    WalPosition(segmentId, offset)

The current single-file WAL uses:

    segmentId = 0

### Why

A bare byte offset works for a single WAL file but becomes ambiguous once the
WAL is segmented.

Using a segment ID preserves direct physical replay semantics while allowing
the WAL to evolve toward:

    segment-N.wal

without changing the snapshot-position contract.

### Gain

- Direct mapping to FileChannel seeking.
- Clear physical recovery location.
- Future WAL segmentation fits naturally.
- Avoids prematurely introducing logical sequence numbering.
- Snapshot recovery remains simple.

### Cost

- Position is coupled to physical storage layout.
- Segment lifecycle rules will eventually be required.
- Compaction may need position translation or segment-retention rules.
- Does not solve logical ordering across replicated nodes.

### Alternative — Bare Byte Offset

Simpler today:

    offset = 12480

Rejected because the value becomes ambiguous with multiple WAL files.

### Alternative — Logical Sequence Number

Example:

    sequence = 10948

Advantages:

- independent of physical WAL layout;
- potentially useful for replication and distributed ordering.

Costs:

- requires sequence allocation semantics;
- requires mapping sequence numbers back to physical storage;
- introduces machinery not needed for current local recovery.

Deferred until the architecture requires logical-log identity.

### Current Constraint

The implementation currently supports one WAL segment only.

Therefore:

    segmentId = 0

No segmentation behavior should be added merely because the abstraction
contains a segment identifier.

### Concurrency Trade-off

The snapshot state and WalPosition must be captured consistently.

Current choice:

    briefly hold the queue state lock
    capture state + durable WalPosition
    release lock
    write snapshot outside the lock

This may briefly pause queue mutations during snapshot capture.

The duration and contention impact have not yet been measured.

### Revisit When

Revisit when:

- snapshot copying becomes expensive;
- lock contention is measured;
- WAL segmentation is implemented;
- dedicated WAL writer/group commit is introduced;
- distributed replication requires logical log indexes.

## v0.12.5 — Segmented WAL

### Decision

Replace unbounded single-file WAL growth with a sequence of WAL segments.

The highest authoritative `.wal` segment is active.

All lower segments are sealed and immutable.

### Motivation

Snapshot-based recovery makes older WAL history logically redundant.

A single WAL file makes physical prefix reclamation difficult because removing
old bytes would require rewriting the remaining active log.

Segments allow future compaction to delete whole immutable files.

### Segment Size Policy

Segment size is a target rather than a hard limit.

A record that causes the target to be exceeded remains entirely in the current
segment.

Rotation occurs before the following append.

### Gain

- Sealed WAL history becomes immutable.
- Frames never cross physical files.
- Future history reclamation can delete whole segments.
- Snapshot WalPosition naturally maps to `(segmentId, offset)`.
- Active-tail crash repair remains localized to the newest segment.
- No separate manifest or ACTIVE flag is currently required.

### Cost

- More files must be discovered and validated.
- Segment continuity becomes an invariant.
- Recovery spans multiple files.
- Rotation introduces additional failure points.
- Atomic filesystem rename is part of correctness.
- Sealed and active segments require different torn-tail policies.

### Authority Trade-off

We derive segment state from filenames:

    highest *.wal = active
    lower *.wal   = sealed
    *.tmp         = non-authoritative

This avoids maintaining separate mutable lifecycle metadata.

The trade-off is tighter dependence on filesystem naming and directory
discovery.

### Recovery Trade-off

Active segment:

    torn tail -> truncate/recover

Sealed segment:

    torn tail -> fail

This is intentionally stricter for sealed history because the existence of a
newer segment proves the old segment should already have been complete before
rotation.

### Rotation Failure Policy

Before atomic promotion:

    old segment remains authoritative.

After atomic promotion:

    new segment is authoritative even if the current process fails to open or
    activate it.

In the latter case, the current WAL instance must stop writing and restart.

### Append Failure Policy

A failed append may leave a partial frame.

The current WAL instance is poisoned so another complete frame can never be
written behind the torn frame.

Restart repairs the active tail before writes resume.

### Deferred

Not included in this version:

- physical deletion of sealed segments;
- segment manifest;
- distributed replication;
- logical record sequence numbers;
- multiple concurrent WAL writers;
- dedicated writer thread;
- group commit.

These should only be introduced when the corresponding requirement appears.

## v0.13.0 — Snapshot-Authorized WAL Segment Reclamation

### Decision

Use the latest successfully promoted snapshot as the sole authority for
whole-segment WAL reclamation. Retain the snapshot's segment and every newer
segment; delete only lower segment IDs.

### Motivation

Without reclamation, segmentation merely converts one unbounded file into an
unbounded directory. Snapshot promotion already establishes a durable recovery
point, so it is the natural authorization boundary for removing redundant WAL
history.

### Gain

- WAL disk usage can shrink without rewriting live log data.
- Planning is deterministic and independently testable.
- The boundary segment remains available for replay from the snapshot offset.
- A partial deletion failure is safe to retry.
- Failed and stale snapshots cannot expose recovery to premature deletion.

### Cost

- Snapshot commit may report a compaction failure after the snapshot itself is
  already authoritative; callers must treat reclamation as retryable cleanup.
- Filesystem deletion errors can leave more history than necessary.
- Whole-segment reclamation may retain unused bytes before the snapshot offset
  in the boundary segment.
- Snapshot commit and deletion are serialized by the coordinator.

### Conservative boundary trade-off

The implementation does not rewrite the boundary segment to recover its unused
prefix. This sacrifices some disk efficiency in exchange for immutable sealed
segments, simpler crash recovery, and a much smaller correctness surface.

### Failure policy

Deletion is not transactional. On failure, already deleted segments stay
deleted and remaining eligible segments stay present. This is safe because the
authoritative snapshot precedes every delete and each target is independently
proven redundant.

### Deferred

- boundary-segment prefix rewriting;
- background/asynchronous reclamation;
- deletion tombstones or a segment manifest;
- retention policies independent of recovery safety;
- coordination with replicas or remote snapshots.

## v0.14.0 — Fail Closed After WAL Prefix Reclamation

### Decision

Allow missing/corrupt snapshot fallback only while the WAL still begins at
segment 0. Once an earlier segment prefix has been reclaimed, require a valid
snapshot and retained suffix as one recovery chain.

### Gain

- prevents silent partial recovery;
- preserves WAL-only recovery before compaction;
- reconstructs monotonic snapshot authority after restart;
- rejects invalid snapshot positions before promotion.

### Cost

- snapshot loss after reclamation is now an availability failure;
- position validation can scan a segment to prove a frame boundary;
- segment numbering becomes part of the recovery-authority model.

### Deferred

- a persistent segment manifest;
- queue-generation or storage-lineage identifiers;
- remote snapshot restoration;
- automatic snapshot and reclamation scheduling.

## v0.15.0 — Force Parent Directories at Authority Boundaries

### Decision

Force the containing directory after snapshot promotion, WAL segment creation
or promotion, and each reclaimed segment deletion. Do not silently accept
filesystems that cannot provide this operation.

### Gain

- aligns reported durability with both file data and filesystem namespace;
- prevents promoted segment names from being treated as durable too early;
- prevents failed snapshot metadata durability from authorizing compaction;
- makes deletion progress durable in the same order it is observed.

### Cost

- adds synchronous filesystem operations to infrequent lifecycle paths;
- directory forcing is filesystem and operating-system dependent;
- failure after atomic promotion is inherently indeterminate and requires
  fail-closed handling rather than rollback claims.

### Deferred

- batching deletion metadata forces;
- a storage abstraction for non-POSIX filesystems;
- filesystem-specific durability certification;
- replicated durability.

## v0.16.0 — Segment-Distance Checkpoint Scheduling

### Decision

Run storage maintenance through a single fixed-delay lifecycle manager. Use
distance in WAL segment IDs from the latest authoritative snapshot as the
initial checkpoint policy.

### Gain

- WAL reclamation no longer depends on application code remembering each
  checkpoint;
- the trigger is derived from durable physical progress and survives restart;
- policy is deterministic and independently testable;
- failed cleanup is retried even without a newer snapshot;
- maintenance failure is isolated from completed queue mutations.

### Cost

- introduces a managed background thread and explicit shutdown responsibility;
- snapshot capture and filesystem maintenance still consume queue/storage
  resources;
- segment distance is an approximate disk bound, not an exact byte bound;
- callers must monitor exposed lifecycle failure state.

### Deferred

- byte-accurate WAL usage policy;
- adaptive or time-based checkpoint triggers;
- asynchronous snapshot serialization from an immutable state view;
- metrics and external health endpoints;
- distributed maintenance ownership.

## v0.17.0 — Embed Storage Lineage in WAL and Snapshots

### Decision

Persist `(queueId, generationId, partitionId)` in every WAL segment and
snapshot, and require exact equality across recovery and compaction authority
transitions. Reject pre-lineage formats instead of adding migration machinery.

### Gain

- prevents valid but unrelated artifacts from being combined during restore;
- prevents a foreign snapshot from authorizing deletion of local WAL history;
- makes queue generation and partition identity explicit before ownership and
  replication are introduced;
- creates a reusable fencing dimension for later placement epochs.

### Cost

- WAL and snapshot formats advance to version 2;
- existing v1 storage cannot be opened by this release;
- UUID metadata increases each WAL segment header and snapshot payload;
- correct backup/restore must preserve the complete lineage tuple.

### Deferred

- migration from v1 artifacts;
- queue namespace and customer tenancy metadata;
- partition routing and ownership epochs;
- replicated generation creation and consensus-backed metadata.

## v0.18.0 — PostgreSQL Metadata Authority

### Decision

Introduce an independently packaged Spring Boot metadata service whose durable
repository is PostgreSQL. Use database transactions for idempotent creation,
conditional updates for lifecycle fencing, and Flyway for schema authority.
Keep message data in the separate `queue-core` module.

### Gain

- provides a shared, durable multi-tenant queue namespace;
- makes ambiguous create retries safe;
- exposes lifecycle progress instead of hiding cross-resource partial work;
- introduces generation and version fencing before distributed provisioning;
- allows multiple future service instances to share metadata authority.

### Cost

- PostgreSQL becomes an operational dependency and availability boundary;
- the service adds a REST API and database connection-pool management concerns;
- metadata and queue storage cannot participate in one atomic transaction;
- schema evolution now requires an explicit production migration strategy;
- Spring Boot becomes part of the control-plane runtime dependency surface.

### Deferred

- queue-node registration and storage provisioning;
- reconciliation of stuck lifecycle states;
- partition placement and ownership epochs;
- authentication, authorization, quotas, and rate limits;
- schema rollback policy and zero-downtime migration compatibility;
- metadata replication beyond PostgreSQL's own deployment model.

## v0.19.0 — Lease-Fenced Queue Provisioning

### Decision

Provision queue storage through a separately deployed queue-node reconciler.
PostgreSQL grants one finite claim for an eligible `PROVISIONING` descriptor
and increments a durable fencing token whenever an expired claim is taken
over. Storage creation is deterministic and bound to the descriptor's exact
queue, generation, and partition lineage. Only the current, unexpired claim
may publish `ACTIVE` or `PROVISIONING_FAILED`.

### Gain

- closes the metadata/filesystem transaction gap through observable,
  retryable reconciliation;
- prevents a delayed worker from publishing lifecycle state after takeover;
- makes worker crash recovery independent of process-local locks;
- exercises leases, fencing, idempotence, and conditional publication across
  service and storage boundaries;
- preserves PostgreSQL as control-plane authority and the WAL as data-plane
  durability authority.

### Cost

- queue readiness becomes asynchronous after a successful create request;
- correctness depends on bounded claim duration and database time;
- a worker that exceeds its lease may finish harmless storage work but cannot
  publish it;
- claim records remain after completion to make ambiguous completion retries
  idempotent;
- the internal provisioning API adds an operational trust boundary.

### Deferred

- claim renewal for long-running provisioning;
- authenticated service-to-service APIs;
- durable node registration and health;
- partition placement and runtime ownership epochs;
- automatic retry policy for `PROVISIONING_FAILED` descriptors;
- deletion reconciliation and storage garbage collection.
