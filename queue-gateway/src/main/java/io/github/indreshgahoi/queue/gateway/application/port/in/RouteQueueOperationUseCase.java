package io.github.indreshgahoi.queue.gateway.application.port.in;

import io.github.indreshgahoi.queue.gateway.domain.model.ForwardResponse;

import java.util.UUID;

public interface RouteQueueOperationUseCase {
    ForwardResponse publish(UUID queueId, String body, String contentType);

    ForwardResponse receive(UUID queueId);

    ForwardResponse ack(UUID queueId, String receiptHandle);

    ForwardResponse nack(
            UUID queueId,
            String receiptHandle,
            String body,
            String contentType
    );
}
