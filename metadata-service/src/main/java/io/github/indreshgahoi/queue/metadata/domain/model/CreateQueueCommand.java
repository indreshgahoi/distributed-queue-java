package io.github.indreshgahoi.queue.metadata.domain.model;

public record CreateQueueCommand(
        String tenantId,
        String queueName,
        String idempotencyKey
) {
    public CreateQueueCommand {
        QueueDescriptor.requireText(tenantId, "tenantId");
        QueueDescriptor.requireText(queueName, "queueName");
        QueueDescriptor.requireText(idempotencyKey, "idempotencyKey");
    }
}
