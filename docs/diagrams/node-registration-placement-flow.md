# Node Registration and Placement Flow

```mermaid
sequenceDiagram
    autonumber
    participant N as Queue Node
    participant M as Metadata Service
    participant DB as PostgreSQL

    N->>M: Register(nodeId, endpoint, lease)
    M->>DB: Upsert registration<br/>and increment registrationEpoch
    DB-->>N: registrationEpoch + leaseExpiresAt

    loop Before registration lease expires
        N->>M: Heartbeat(nodeId, registrationEpoch)
        M->>DB: Renew only matching unexpired epoch
        DB-->>N: new leaseExpiresAt
    end

    N->>M: Claim(nodeId, registrationEpoch)
    M->>DB: Verify live registration
    M->>DB: Lock oldest unplaced PROVISIONING queue
    M->>DB: Select live node with fewest placements
    M->>DB: Insert placement epoch 1
    M->>DB: Select claimable placement assigned to caller

    alt Caller is assigned node
        DB-->>N: lineage + registrationEpoch<br/>placementEpoch + fencingToken
        N->>N: Create or validate lineage-bound WAL
        N->>M: Complete with all authority dimensions
        M->>DB: Revalidate node lease, registration epoch,<br/>placement epoch, claim token and lease
        DB-->>N: ACTIVE
    else Caller is not assigned node
        DB-->>N: No claim
    end
```

The authority dimensions solve different stale-actor problems:

```text
generationId       fences delete and recreate
registrationEpoch  fences old processes using the same nodeId
placementEpoch     fences old partition assignments
fencingToken       fences old provisioning attempts
metadataVersion    fences stale metadata updates
```

