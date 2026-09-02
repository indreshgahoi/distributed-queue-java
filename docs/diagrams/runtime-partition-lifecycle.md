# Runtime Partition Lifecycle

The durable queue lifecycle and node-local runtime lifecycle are related but
not identical. `ACTIVE` makes a partition eligible for recovery; only a fenced
`READY` publication makes the recovered instance observable as ready.

```mermaid
stateDiagram-v2
    [*] --> Unplaced: queue created / PROVISIONING
    Unplaced --> Placed: durable placement created
    Placed --> Provisioning: assigned node claims work
    Provisioning --> ActiveInactive: storage materialized / ACTIVE
    Provisioning --> ProvisioningFailed: materialization fails

    ActiveInactive --> Recovering: live assigned node discovers placement
    Recovering --> Ready: recovery succeeds / fenced READY commits
    Recovering --> RuntimeFailed: recovery fails / fenced FAILED commits
    Recovering --> ActiveInactive: authority changes / stale result closed

    Ready --> Ready: same authority retained
    Ready --> ActiveInactive: registration lost or placement superseded
    RuntimeFailed --> Recovering: reconciliation retries

    ActiveInactive --> [*]: queue leaves ACTIVE
    Ready --> [*]: node shutdown / runtime closed
    ProvisioningFailed --> [*]
```

## Reconciliation decision flow

```mermaid
flowchart TD
    A[Reconciliation tick] --> B{Current registration exists and is unexpired?}
    B -- No --> C[Close all active runtimes]
    B -- Yes --> D[Request ACTIVE placements for node incarnation]
    D --> E{Already active under identical authority?}
    E -- Yes --> F[Retain runtime]
    E -- No --> G[Open snapshot and WAL / validate lineage / recover]
    G --> H{Local registration still matches?}
    H -- No --> I[Close stale recovery]
    H -- Yes --> J[Publish READY with full identity]
    J --> K{PostgreSQL confirms ACTIVE, registration, and placement?}
    K -- No --> I
    K -- Yes --> L[Install runtime in active map]
    G -. recovery failure .-> M[Publish FAILED only if authority is current]
```

The active map is downstream of PostgreSQL's final validation so a slow
recovery result cannot become usable after its authority was superseded.
