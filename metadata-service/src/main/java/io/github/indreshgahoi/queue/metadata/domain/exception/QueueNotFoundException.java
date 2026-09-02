package io.github.indreshgahoi.queue.metadata.domain.exception;

import java.util.UUID;

public final class QueueNotFoundException
        extends QueueMetadataException {
    public QueueNotFoundException(String tenantId, String queueName) {
        super("Queue does not exist: " + tenantId + "/" + queueName);
    }

    public QueueNotFoundException(UUID queueId) {
        super("Queue does not exist: " + queueId);
    }
}
