# Provisioning Claim Decision Flow

This flow describes one `ProvisioningReconciler.runOnce()` invocation and how
the durable claim rules control its outcome.

```mermaid
flowchart TD
    A[Scheduled reconciliation tick] --> B[Request claim with worker ID<br/>and lease duration]
    B --> C{Eligible PROVISIONING<br/>queue exists?}
    C -- No --> D[Return false;<br/>wait for next tick]
    C -- Yes --> E[PostgreSQL locks candidate<br/>with SKIP LOCKED]
    E --> F[Insert claim token 1<br/>or increment token on takeover]
    F --> G[Return assignment containing<br/>queue lineage, worker, token,<br/>and lease expiry]
    G --> H[Create or open deterministic<br/>lineage-bound WAL directory]
    H --> I{Storage provisioned<br/>and lineage valid?}

    I -- No --> J[Report failure using<br/>the same claim identity]
    J --> K{Claim still current<br/>and unexpired?}
    K -- Yes --> L[Transition to<br/>PROVISIONING_FAILED]
    K -- No --> M[Reject stale failure;<br/>do not overwrite newer authority]
    L --> N[Throw ProvisioningException;<br/>scheduler logs it]
    M --> N

    I -- Yes --> O[Request completion using<br/>queue ID, generation ID,<br/>partition ID, worker ID, token]
    O --> P[Lock queue and claim rows]
    P --> Q{Same claim already<br/>published ACTIVE?}
    Q -- Yes --> R[Return existing ACTIVE<br/>idempotently]
    Q -- No --> S{Full claim identity matches,<br/>state is PROVISIONING,<br/>and lease is unexpired?}
    S -- Yes --> T[Transition to ACTIVE and<br/>increment metadataVersion]
    S -- No --> U[Reject with claim lost;<br/>stale worker is fenced]
    T --> V[Return true]
    R --> V
    U --> J

    W[Worker crashes or pauses] -. no completion .-> X[Lease expires]
    X --> Y[Another worker claims queue]
    Y --> Z[Persist higher fencing token]
    Z --> G
```

## Authority carried by a claim

A completion request must reproduce the entire persisted identity:

```text
(queueId, generationId, partitionId, workerId, fencingToken)
```

The lease answers *when another worker may take over*. The fencing token
answers *which worker result may still be published*. The storage lineage
prevents retry or takeover from opening storage that belongs to another queue
generation or partition.
