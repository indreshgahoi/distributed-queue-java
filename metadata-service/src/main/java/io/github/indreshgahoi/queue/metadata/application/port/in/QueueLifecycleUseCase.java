package io.github.indreshgahoi.queue.metadata.application.port.in;

import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;

public interface QueueLifecycleUseCase {

    QueueDescriptor completeProvisioning(QueueDescriptor expected);

    QueueDescriptor failProvisioning(QueueDescriptor expected);

    QueueDescriptor retryProvisioning(QueueDescriptor expected);

    QueueDescriptor completeDeletion(QueueDescriptor expected);

    QueueDescriptor failDeletion(QueueDescriptor expected);

    QueueDescriptor retryDeletion(QueueDescriptor expected);
}
