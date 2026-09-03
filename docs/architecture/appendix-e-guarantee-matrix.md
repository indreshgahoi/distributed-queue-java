# Appendix E — Guarantee Matrix

## Status vocabulary

| Status | Meaning |
|---|---|
| Current | Implemented and tested in the repository |
| Planned | Assigned to a concrete future milestone |
| Deferred | Intentionally outside the current roadmap |

## Durability and replication

| Guarantee | Status | Planned phase |
|---|---|---|
| Local WAL record is forced before local mutation succeeds | Current | Existing |
| Follower rejects wrong lineage, gaps, conflicts, and stale epoch | Current | v0.26 |
| Bounded follower HTTP batch and idempotent retry | Current development | v0.27 |
| Logical term/index survives WAL reclamation | Planned | v0.28 |
| Replication factor defaults to three | Planned | v0.29 |
| Assigned learner automatically catches up | Planned | v0.30 |
| Client success waits for durable majority | Planned | v0.31 |
| Follower applies only committed records | Planned | v0.31 |
| Leader election continues without PostgreSQL | Planned | v0.32 |
| Failed voter is replaced safely | Planned | v0.33 |
| Exactly-once processing | Deferred | None |

## Operation commit policy target

| Operation | Log entry required? | Majority before success? | Why |
|---|---:|---:|---|
| Publish | Yes | Yes | Otherwise acknowledged message may disappear |
| Receive/lease start | Yes | Yes | Otherwise two leaders may deliver the same attempt concurrently |
| ACK | Yes | Yes | Otherwise acknowledged message may reappear after failover |
| NACK | Yes | Yes | Otherwise retry timing/state may roll back |
| Lease expiry | Yes | Yes | Otherwise redelivery decision may differ after failover |
| DLQ transition | Yes | Yes | Otherwise terminal retry outcome may roll back |
| Read metrics | No | No | Metrics are approximate observations |

## Failure matrix for replication factor three

| Reachable voting replicas | Mutation availability | Safety behavior |
|---:|---|---|
| 3 | Available | Commit after any 2 durable copies |
| 2 | Available | Commit requires both reachable replicas |
| 1 | Unavailable | Reject mutation; cannot form majority |
| 0 | Unavailable | No service |

This assumes a valid elected leader among the reachable majority. Two isolated
minority nodes cannot independently commit because neither owns two votes.

## Component outage target

| Outage | Existing established partition | Administrative operations |
|---|---|---|
| One follower | Continue with majority | Replacement waits or starts |
| Leader | Election if majority survives | Metadata observes new leader |
| PostgreSQL | Replica quorum continues; discovery may use bounded cache | Create/delete/rebalance unavailable |
| Gateway | Direct node service may exist internally; customer endpoint unavailable | Unaffected |
| Snapshot transfer target | Foreground quorum continues if capacity allows | Replica recovery delayed |

## Multi-partition semantics target

| Property | Guarantee |
|---|---|
| Message placement | Exactly one deterministic partition per publish attempt |
| Ordering | Per partition; optionally per message group |
| Queue-wide order | Not guaranteed |
| Receive fairness | Best effort with bounded rotating probes |
| Empty receive | May be approximate when not all partitions are probed |
| ACK/NACK routing | Exact originating partition through authenticated receipt metadata |
| Partition resize | Deferred; count immutable per generation initially |

## Benchmark gates

| Before phase | Required evidence |
|---|---|
| v0.28 implementation | v0.27.1 forced append, follower batch, snapshot interference, and partition-density baseline |
| v0.30 scheduler | Per-node worker/connection scaling and slow-follower backpressure results |
| v0.31 quorum acknowledgement | End-to-end p99 decomposition: leader force, network, follower force, commit/apply |
| v0.33 repair | Foreground latency during snapshot transfer and mass node recovery |
| Multi-partition | Routing distribution, bounded receive hit rate, and hot-partition behavior |

Performance never changes a guarantee silently. Any relaxation requires a new
named acknowledgement policy, semantics update, tests, and ADR.
