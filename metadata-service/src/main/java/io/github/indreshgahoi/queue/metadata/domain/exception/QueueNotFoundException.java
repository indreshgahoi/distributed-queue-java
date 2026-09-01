package io.github.indreshgahoi.queue.metadata.domain.exception;

public final class QueueNotFoundException
        extends QueueMetadataException {
    public QueueNotFoundException(String tenantId, String queueName) {
        super("Queue does not exist: " + tenantId + "/" + queueName);
    }
}
