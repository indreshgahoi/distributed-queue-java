package io.github.indreshgahoi.queue.node.application.port.in;

import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

public interface QueueDataPlaneUseCase {
    String publish(UUID queueId, String payload);

    Optional<MessageDelivery> receive(UUID queueId);

    boolean ack(UUID queueId, String receiptHandle);

    boolean nack(UUID queueId, String receiptHandle, Duration retryDelay);
}
