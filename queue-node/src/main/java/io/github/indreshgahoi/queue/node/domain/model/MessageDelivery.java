package io.github.indreshgahoi.queue.node.domain.model;

import java.util.Objects;

public record MessageDelivery(
        String messageId,
        String payload,
        String receiptHandle,
        int attempt
) {
    public MessageDelivery {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(receiptHandle, "receiptHandle");
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be positive");
        }
    }
}
