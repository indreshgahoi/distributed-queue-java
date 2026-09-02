# ADR 0022: Per-Partition Runtime Admission and Draining

## Status

Accepted for v0.23.0.

## Context

v0.22 correctly ordered data-plane operations against runtime closure by
holding the `RuntimePartitionManager` monitor through the complete callback.
That monitor also covered every other runtime on the node. A slow WAL force for
one queue therefore delayed unrelated queues and lifecycle reconciliation.

The v0.22.1 baseline confirmed that READY lookup is independent of active queue
count, but concurrent admission through the node-wide monitor did not scale.
The lock scope, rather than the serving-index lookup, was the remaining local
isolation problem.

## Decision

Represent every active partition with a `RuntimePartitionHandle` containing
its immutable authority identity, runtime queue, lifecycle state, and active
operation count.

An operation obtains the handle from a concurrent queue-ID index and atomically
acquires a handle-local permit only while the handle is `READY`. It revalidates
the registration lease and epoch, then performs the queue callback without
holding the handle lock or manager monitor. Releasing the permit decrements the
active count in a `finally`-equivalent `AutoCloseable` path.

Deactivation removes the exact handle from the serving index, changes it to
`CLOSING`, rejects new permits, waits for admitted operations to drain, closes
the queue exactly once, and publishes `CLOSED` locally. A request that already
read the handle cannot bypass closure because permit acquisition checks the
handle state under the same lock used to begin closing.

Authority is evaluated at admission. An operation admitted while the matching
registration is live may complete after reconciliation observes authority
loss; closure is ordered after that operation. Returning failure after a
durable mutation would not undo the mutation and would encourage duplicate
retries.

The manager monitor continues to serialize reconciliation and its authoritative
lineage-keyed maps. It is no longer acquired by ordinary data-plane operations.

## Consequences

- slow storage on one partition does not block operations on another;
- closure of one partition may drain concurrently with operations elsewhere;
- operation and close remain strictly ordered for the same partition;
- stale references obtained before index removal cannot acquire a new permit;
- concurrent close attempts close the underlying runtime exactly once;
- reconciliation itself remains single-threaded;
- shutdown and deactivation can wait indefinitely for a stuck admitted
  operation because bounded drain policy is not yet defined;
- operations within one `LocalMessageQueue` retain its existing lock and WAL
  serialization behavior.

## Alternatives Considered

### Per-partition read/write lock

Viable, but rejected because an explicit permit count exposes the drain state
needed for later shutdown deadlines, observability, and admission limits.

### Release the manager lock after returning RuntimeQueue

Rejected because the queue reference could outlive its authority handle and be
used after closure.

### Keep the node-wide monitor

Rejected because measured contention and the WAL failure model show that an
unrelated queue can become part of another queue's latency domain.

### Parallelize reconciliation as part of this milestone

Deferred. The correctness problem is data-plane isolation. Parallel recovery
and lifecycle scheduling require their own resource limits and failure policy.
