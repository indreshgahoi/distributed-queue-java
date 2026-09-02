# Stable Data-Plane Routing Sequence

```mermaid
sequenceDiagram
    participant C as Customer
    participant G as Queue Gateway
    participant M as Metadata Service
    participant P as PostgreSQL
    participant N as Queue Node
    participant W as Local WAL

    C->>G: POST /v1/queues/{queueId}/messages
    G->>M: GET /internal/v1/routes/queues/{queueId}
    M->>P: Resolve ACTIVE + placement + live node + READY epochs
    P-->>M: Fenced QueueRoute
    M-->>G: nodeEndpoint and authority identity
    G->>N: Forward request once
    N->>N: Validate local READY admission and registration
    N->>W: Append durable transition
    W-->>N: Durable
    N-->>G: 201 / 204 / 4xx / 5xx
    G-->>C: Preserve downstream response
```

If the node call fails after it may have reached the WAL, the gateway returns
an ambiguous `502 Bad Gateway`. It does not resolve another route or replay the
mutation. The customer must decide whether and how to retry under the queue's
documented at-least-once semantics.
