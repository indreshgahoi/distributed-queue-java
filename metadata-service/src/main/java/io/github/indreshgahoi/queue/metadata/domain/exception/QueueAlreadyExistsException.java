package io.github.indreshgahoi.queue.metadata.domain.exception;

public final class QueueAlreadyExistsException
        extends QueueMetadataException {

    public QueueAlreadyExistsException(
            String tenantId,
            String queueName
    ) {
        super(
                "Queue already exists: "
                        + tenantId
                        + "/"
                        + queueName
        );
    }
}
