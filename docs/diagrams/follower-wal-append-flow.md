# Follower WAL Append Flow

```mermaid
flowchart TD
    A[Receive replicated WAL entry] --> B{Lineage matches?}
    B -- No --> X[Reject lineage mismatch]
    B -- Yes --> C{Epoch is stale?}
    C -- Yes --> Y[Reject stale leader]
    C -- No --> D{Sequence relative to next?}
    D -- Higher / gap --> Z[Reject; request missing sequence]
    D -- Already stored --> E{Record exactly equal?}
    E -- No --> F[Reject conflicting history]
    E -- Yes --> G[Durably advance epoch if newer]
    G --> H[Return ALREADY_PRESENT]
    D -- Exact next --> I[Durably advance epoch if newer]
    I --> J[Append and force existing WAL]
    J --> K[Return APPENDED]
    I -- Storage failure --> P[Poison follower writer]
    J -- Storage failure --> P
```

The epoch fence deliberately becomes durable before the record. If a crash
occurs between those steps, the restarted follower still rejects the obsolete
leader and the current leader can retry the unchanged next sequence.
