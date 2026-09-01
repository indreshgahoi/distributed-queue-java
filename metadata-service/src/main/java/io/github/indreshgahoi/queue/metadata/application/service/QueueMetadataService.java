package io.github.indreshgahoi.queue.metadata.application.service;

import io.github.indreshgahoi.queue.metadata.application.port.in.QueueCatalogUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.in.QueueLifecycleUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.out.QueueMetadataRepository;
import io.github.indreshgahoi.queue.metadata.domain.model.CreateQueueCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueLifecycleState;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
final class QueueMetadataService
        implements QueueCatalogUseCase, QueueLifecycleUseCase {

    private final QueueMetadataRepository repository;

    QueueMetadataService(
            QueueMetadataRepository repository
    ) {
        this.repository = Objects.requireNonNull(
                repository,
                "repository"
        );
    }

    public QueueDescriptor createQueue(
            CreateQueueCommand command
    ) {
        return repository.create(command);
    }

    public Optional<QueueDescriptor> getQueue(
            String tenantId,
            String queueName
    ) {
        return repository.find(tenantId, queueName);
    }

    public List<QueueDescriptor> listQueues(
            String tenantId
    ) {
        return repository.list(tenantId);
    }

    public QueueDescriptor beginDeleteQueue(
            String tenantId,
            String queueName
    ) {
        return repository.beginDeletion(tenantId, queueName);
    }

    public QueueDescriptor completeProvisioning(
            QueueDescriptor expected
    ) {
        return transition(
                expected,
                QueueLifecycleState.PROVISIONING,
                QueueLifecycleState.ACTIVE
        );
    }

    public QueueDescriptor failProvisioning(
            QueueDescriptor expected
    ) {
        return transition(
                expected,
                QueueLifecycleState.PROVISIONING,
                QueueLifecycleState.PROVISIONING_FAILED
        );
    }

    public QueueDescriptor retryProvisioning(
            QueueDescriptor expected
    ) {
        return transition(
                expected,
                QueueLifecycleState.PROVISIONING_FAILED,
                QueueLifecycleState.PROVISIONING
        );
    }

    public QueueDescriptor completeDeletion(
            QueueDescriptor expected
    ) {
        return transition(
                expected,
                QueueLifecycleState.DELETING,
                QueueLifecycleState.DELETED
        );
    }

    public QueueDescriptor failDeletion(
            QueueDescriptor expected
    ) {
        return transition(
                expected,
                QueueLifecycleState.DELETING,
                QueueLifecycleState.DELETE_FAILED
        );
    }

    public QueueDescriptor retryDeletion(
            QueueDescriptor expected
    ) {
        return transition(
                expected,
                QueueLifecycleState.DELETE_FAILED,
                QueueLifecycleState.DELETING
        );
    }

    private QueueDescriptor transition(
            QueueDescriptor expected,
            QueueLifecycleState expectedState,
            QueueLifecycleState nextState
    ) {
        Objects.requireNonNull(expected, "expected");
        if (expected.lifecycleState() != expectedState) {
            throw new IllegalArgumentException(
                    "Expected descriptor state " + expectedState
            );
        }
        return repository.transition(
                expected.queueId(),
                expected.generationId(),
                expected.metadataVersion(),
                expectedState,
                nextState
        );
    }
}
