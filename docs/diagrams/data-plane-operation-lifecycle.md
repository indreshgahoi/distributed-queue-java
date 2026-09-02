# Data-Plane Operation Lifecycle

> This diagram records the v0.22 node-wide lifecycle guard. ADR 0022 replaces
> that lock scope in v0.23; see the
> [per-partition runtime lifecycle](per-partition-runtime-lifecycle.md).

The runtime manager is both the READY registry and the local lifecycle guard.
Controllers never receive a queue reference that can outlive the guard.

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Queue-node HTTP
    participant M as RuntimePartitionManager
    participant R as LocalMessageQueue
    participant W as WAL
    participant X as Reconciler / shutdown

    C->>H: publish / receive / ACK / NACK
    H->>M: withReadyQueue(queueId, operation)
    M->>M: acquire lifecycle monitor
    M->>M: find installed runtime
    M->>M: validate registration epoch + lease
    alt runtime unavailable
        M-->>H: RuntimePartitionUnavailable
        H-->>C: 503 Problem Detail
    else runtime admitted
        M->>R: execute operation while guarded
        R->>W: append + force durable transition
        W-->>R: committed
        R-->>M: operation result
        Note over X,M: deactivation waits for monitor
        M->>M: release lifecycle monitor
        M-->>H: result
        H-->>C: 201 / 200 / 204
        X->>M: acquire monitor, close if superseded
    end
```

## Race ordering

```mermaid
flowchart TD
    A[Operation and deactivation race] --> B{Who acquires lifecycle guard first?}
    B -- Operation --> C[Validate READY runtime and registration]
    C --> D[Complete durable queue transition]
    D --> E[Release guard]
    E --> F[Deactivation closes runtime]
    B -- Deactivation --> G[Remove and close runtime]
    G --> H[Later operation sees no READY runtime]
    H --> I[Return 503 without mutation]
```
