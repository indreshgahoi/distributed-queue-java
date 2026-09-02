# Architecture Diagrams

These diagrams explain cross-component protocols whose correctness depends on
ordering, authority, or failure handling.

- [Provisioning claim sequence](provisioning-claim-sequence.md) — interactions
  between the queue node, metadata service, PostgreSQL, and local WAL storage.
- [Provisioning claim decision flow](provisioning-claim-flow.md) — decisions in
  one reconciliation cycle, including failures, lease expiry, and takeover.
- [Node registration and placement flow](node-registration-placement-flow.md) —
  process-incarnation fencing, heartbeat renewal, initial placement, and
  placement-aware provisioning.
- [Runtime partition lifecycle](runtime-partition-lifecycle.md) — durable queue
  progression, fenced recovery, readiness publication, and deactivation.
- [Data-plane operation lifecycle](data-plane-operation-lifecycle.md) — guarded
  request admission, WAL delegation, and deterministic closure race ordering.
- [Follower WAL append flow](follower-wal-append-flow.md) — lineage, sequence,
  retry, conflict, and durable leader-epoch fencing decisions.
