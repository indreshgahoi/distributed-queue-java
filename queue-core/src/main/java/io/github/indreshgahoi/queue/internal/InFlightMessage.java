package io.github.indreshgahoi.queue.internal;

import io.github.indreshgahoi.queue.Message;

import java.time.Instant;

public record InFlightMessage(
        Message message,
        String receiptHandle,
        Instant leaseUntil,
        int attempt
) {
}