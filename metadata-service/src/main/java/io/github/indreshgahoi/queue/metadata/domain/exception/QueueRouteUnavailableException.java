package io.github.indreshgahoi.queue.metadata.domain.exception;

import java.util.UUID;

public final class QueueRouteUnavailableException
        extends QueueMetadataException {
    public QueueRouteUnavailableException(UUID queueId) {
        super("Queue has no authoritative READY route: " + queueId);
    }
}
