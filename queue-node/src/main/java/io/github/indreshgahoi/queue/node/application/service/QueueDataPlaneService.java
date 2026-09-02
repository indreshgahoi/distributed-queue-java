package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.in.QueueDataPlaneUseCase;
import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class QueueDataPlaneService implements QueueDataPlaneUseCase {
    private final RuntimePartitionManager partitions;

    public QueueDataPlaneService(RuntimePartitionManager partitions) {
        this.partitions = Objects.requireNonNull(partitions, "partitions");
    }

    @Override
    public String publish(UUID queueId, String payload) {
        Objects.requireNonNull(payload, "payload");
        return partitions.withReadyQueue(queueId, queue -> queue.publish(payload));
    }

    @Override
    public Optional<MessageDelivery> receive(UUID queueId) {
        return partitions.withReadyQueue(queueId, queue -> queue.receive());
    }

    @Override
    public boolean ack(UUID queueId, String receiptHandle) {
        Objects.requireNonNull(receiptHandle, "receiptHandle");
        return partitions.withReadyQueue(
                queueId,
                queue -> queue.ack(receiptHandle)
        );
    }

    @Override
    public boolean nack(
            UUID queueId,
            String receiptHandle,
            Duration retryDelay
    ) {
        Objects.requireNonNull(receiptHandle, "receiptHandle");
        Objects.requireNonNull(retryDelay, "retryDelay");
        if (retryDelay.isNegative()) {
            throw new IllegalArgumentException(
                    "retryDelay must not be negative"
            );
        }
        return partitions.withReadyQueue(
                queueId,
                queue -> queue.nack(receiptHandle, retryDelay)
        );
    }
}
