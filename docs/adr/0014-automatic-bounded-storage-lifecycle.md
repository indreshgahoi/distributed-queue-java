# ADR 0014: Automatic Bounded Storage Lifecycle

## Status

Accepted for v0.16.0.

## Context

Snapshots, segmented WAL, and safe reclamation exist, but v0.15.0 still
requires application code to decide when to capture and commit a snapshot.
If no caller performs that orchestration, the WAL directory grows without
bound during otherwise correct operation.

The trigger must be recoverable and explainable. A process-local operation
counter is lost at restart, queue depth does not measure obsolete history, and
wall-clock scheduling alone does not relate directly to disk growth.

## Decision

Introduce a lifecycle manager with a pluggable checkpoint policy. The initial
policy requests a checkpoint when the current WAL segment ID is a configured
distance beyond the latest authoritative snapshot segment.

The manager runs cycles on one fixed-delay daemon thread and also exposes a
deterministic `runOnce()` operation. Both paths are serialized.

Each checkpoint follows the existing authority protocol:

    inspect durable WAL position
        -> evaluate policy
        -> capture queue snapshot
        -> validate and promote snapshot
        -> reclaim snapshot-covered segments

An existing snapshot causes an initial compaction reconciliation cycle. If
snapshot commit succeeds but compaction fails, the manager retries compaction
without waiting for more WAL progress. If snapshot save fails before authority
changes, a later cycle captures fresh state.

## Failure Isolation

Maintenance occurs after queue transitions have crossed their WAL durability
boundary. A maintenance failure therefore cannot retroactively fail those
operations. Scheduled exceptions are contained so one transient failure does
not cancel future cycles. The latest failure remains observable.

## Consequences

### Positive

- normal operation now advances the snapshot/reclamation lifecycle;
- the policy is based on durable, restart-stable progress;
- snapshot and cleanup retry semantics are explicit;
- policy remains replaceable without changing storage mechanisms;
- end-to-end recovery can be tested after automatic reclamation.

### Negative

- lifecycle ownership and shutdown become application responsibilities;
- one background thread is introduced;
- snapshot capture may briefly contend with queue operations;
- segment distance does not account for unusually large records;
- failure observation is currently pull-based.

## Alternatives Considered

### Snapshot after a number of queue operations

Rejected because the counter is volatile unless another durable metadata
mechanism is introduced.

### Snapshot only on a wall-clock interval

Rejected as the initial policy because identical time intervals can represent
very different WAL growth.

### Trigger from queue depth

Rejected because a small live queue can still have extensive obsolete WAL
history.

### Integrate checkpointing directly into every queue operation

Rejected because filesystem maintenance should not extend the success/failure
contract of a queue mutation that is already durably committed.

## Revisit When

- exact disk-usage bounds become necessary;
- snapshot capture contention is measurable;
- health reporting or metrics are introduced;
- multiple processes or replicas require exclusive maintenance ownership.
