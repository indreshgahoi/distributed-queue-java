package io.github.indreshgahoi.queue.gateway.domain.exception;

import java.util.UUID;

public final class QueueRouteUnavailableException extends RuntimeException {
    public QueueRouteUnavailableException(UUID queueId) {
        super("Queue has no authoritative READY route: " + queueId);
    }
}
