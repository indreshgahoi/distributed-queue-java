package io.github.indreshgahoi.queue.metadata.domain.exception;

public final class IdempotencyConflictException
        extends QueueMetadataException {

    public IdempotencyConflictException() {
        super("Idempotency key was already used for another request");
    }
}
