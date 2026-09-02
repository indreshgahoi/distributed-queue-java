# Bounded Admission Lifecycle

```mermaid
flowchart TD
    A[Publish request] --> B[Encode payload as UTF-8]
    B --> C{Payload within per-message limit?}
    C -- No --> D[Reject 413 before WAL append]
    C -- Yes --> E[Acquire queue mutation lock]
    E --> F{Retained count has capacity?}
    F -- No --> G[Reject 429 before WAL append]
    F -- Yes --> H{Retained bytes have capacity?}
    H -- No --> G
    H -- Yes --> I[Append PUBLISH to WAL]
    I --> J[Add message to READY]
    J --> K[Increment retained count and bytes]
    K --> L[Return message identity]

    M[Successful ACK WAL append] --> N[Remove IN_FLIGHT message]
    N --> O[Decrement retained count and bytes]

    P[Recovery] --> Q[Restore snapshot and replay WAL suffix]
    Q --> R[Materialize non-DONE messages]
    R --> S[Rebuild retained count and bytes]
```

The admission check and corresponding state increment share the queue mutation
lock. A concurrent publish therefore observes either the capacity before the
first publish or the capacity after it; both cannot consume the same remaining
slot.
