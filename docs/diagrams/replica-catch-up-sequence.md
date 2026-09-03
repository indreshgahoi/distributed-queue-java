# Replica Catch-Up Sequence

```mermaid
sequenceDiagram
    participant S as Catch-up scheduler
    participant C as ReplicaCatchUpService
    participant L as LeaderReplicationSource
    participant H as Follower HTTP endpoint
    participant F as OrderedFollowerReplicaLog
    participant W as Follower WAL

    S->>C: runOnce(follower, lineage, epoch, nextSequence)
    C->>L: read(nextSequence, boundedLimit)
    L-->>C: consecutive WAL records
    C->>H: POST bounded batch
    H->>F: appendBatch(entries)
    loop each record
        F->>W: append + force
        W-->>F: durable
    end
    F-->>H: acceptedThrough, appended, alreadyPresent
    H-->>C: 200 durable progress
    C-->>S: one-cycle result
```

If the HTTP response is lost, the scheduler may retry the same batch. The
follower recognizes its durable prefix or complete batch and does not append
those records again.
