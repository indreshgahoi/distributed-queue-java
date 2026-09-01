# Provisioning Claim Sequence

This sequence shows the successful path and the important ambiguous-response
path. PostgreSQL is the durable authority for the claim; the fencing token is
checked again when completion is published.

```mermaid
sequenceDiagram
    autonumber
    participant S as ProvisioningScheduler
    participant R as ProvisioningReconciler
    participant H as Metadata HTTP Adapter
    participant M as Metadata Service
    participant DB as PostgreSQL
    participant FS as Lineage-bound WAL Storage

    S->>R: runOnce()
    R->>H: POST /internal/v1/provisioning/claims<br/>{workerId, leaseSeconds}
    H->>M: claim(command)
    M->>DB: BEGIN
    M->>DB: Select oldest PROVISIONING queue<br/>without an unexpired claim<br/>FOR UPDATE SKIP LOCKED

    alt No eligible descriptor
        DB-->>M: no row
        M->>DB: COMMIT
        M-->>H: Optional.empty()
        H-->>R: 204 No Content
        R-->>S: false
    else Descriptor is eligible
        DB-->>M: queue descriptor locked
        M->>DB: Insert claim with token 1,<br/>or increment token on takeover
        DB-->>M: fencingToken
        M->>DB: COMMIT
        M-->>H: claim + queue lineage + lease expiry
        H-->>R: 200 ProvisioningAssignment

        R->>FS: provision(queueId, generationId, partitionId)
        FS->>FS: Open/create deterministic WAL path
        FS->>FS: Validate persisted lineage
        FS-->>R: storage ready

        R->>H: POST claims/{queueId}/complete<br/>{generationId, partitionId,<br/>workerId, fencingToken}
        H->>M: complete(claim identity)
        M->>DB: BEGIN + lock queue and claim
        M->>DB: Verify full identity, PROVISIONING state,<br/>and leaseExpiresAt > database time

        alt Claim is still authoritative
            M->>DB: Transition PROVISIONING -> ACTIVE<br/>and increment metadataVersion
            M->>DB: COMMIT
            M-->>H: ACTIVE descriptor
            H-->>R: 200 OK
            R-->>S: true
        else Same completion already committed
            DB-->>M: ACTIVE + same persisted claim
            M->>DB: COMMIT without another transition
            M-->>H: existing ACTIVE descriptor
            H-->>R: 200 OK (idempotent replay)
            R-->>S: true
        else Claim expired, changed, or lineage differs
            M->>DB: ROLLBACK
            M-->>H: ProvisioningClaimLostException
            H-->>R: 409 Conflict
            R->>H: POST claims/{queueId}/fail<br/>with the same stale identity
            H-->>R: 409 Conflict
            R-->>S: ProvisioningException
            Note over S,R: Scheduler logs the failure.<br/>A later cycle may claim the queue<br/>with a higher fencing token.
        end
    end
```

The WAL creation and metadata transition are intentionally not one atomic
transaction. Safety comes from deterministic, lineage-validated storage and a
fenced publication step; progress comes from lease expiry and retry.
