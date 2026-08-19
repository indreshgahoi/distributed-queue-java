# Queue Semantics

## Version 0.1

### Scope

A single-process, in-memory FIFO queue.

### Guarantees

G1. Every successfully published message is assigned
a unique message identifier.

G2. Messages are returned in publication order.

G3. Receiving a message removes it from the queue.

G4. Receiving from an empty queue returns no message.

### Explicit Non-Guarantees

v0.1 provides no guarantee for:

- durability
- acknowledgement
- redelivery
- retries
- concurrent access
- consumer failure recovery
- process failure recovery
- distributed execution

## Known Failure

If a consumer receives a message and crashes before
processing it, the message is lost.

This limitation will motivate acknowledgement semantics
in a later version.