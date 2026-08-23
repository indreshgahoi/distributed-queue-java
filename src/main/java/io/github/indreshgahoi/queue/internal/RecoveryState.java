package io.github.indreshgahoi.queue.internal;

import io.github.indreshgahoi.queue.Message;

import java.time.Instant;

public record RecoveryState(
            Message message,
            RecoveryStatus status,
            String receiptHandle,
            int attempt,
            Instant leaseUntil,
            Instant retryAt
    ) {
    }