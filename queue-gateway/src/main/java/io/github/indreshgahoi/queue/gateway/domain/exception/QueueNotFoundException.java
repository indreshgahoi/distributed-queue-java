package io.github.indreshgahoi.queue.gateway.domain.exception;

import java.util.UUID;

public final class QueueNotFoundException extends RuntimeException {
    public QueueNotFoundException(UUID queueId) {
        super("Queue does not exist: " + queueId);
    }
}
