package io.github.indreshgahoi.queue.metadata.application.port.in;

import io.github.indreshgahoi.queue.metadata.domain.model.CreateQueueCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;

import java.util.List;
import java.util.Optional;

public interface QueueCatalogUseCase {

    QueueDescriptor createQueue(CreateQueueCommand command);

    Optional<QueueDescriptor> getQueue(
            String tenantId,
            String queueName
    );

    List<QueueDescriptor> listQueues(String tenantId);

    QueueDescriptor beginDeleteQueue(
            String tenantId,
            String queueName
    );
}
