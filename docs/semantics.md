# Queue Semantics

## Version 0.1

### Scope

A single-process, in-memory FIFO queue.

### Guarantees

G1. Every successful message get a unique ID.

G2. Messages are received in publication order.

G3. receive() removes the head message from the queue.

G4. receive() on empty queue returns no messages.

G5. v0.1 is a single threaded and in-memory only.

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
```text
Consumer receive M1 -> M1 is removed -> Consumer crashes -> M1 is permanently lost
```

             


This limitation will motivate acknowledgement semantics
in a later version.

## Version 0.2 — Explicit acknowledgement

### New guarantees

G6. `receive()` does not permanently delete a message.

G7. A received message moves from READY to IN_FLIGHT.

G8. An IN_FLIGHT message is not available for another receive.

G9. `ack()` permanently removes an IN_FLIGHT message.

```text
             receive
READY --------------------> IN_FLIGHT
                                |
                                | ack
                                v
                              DONE
```