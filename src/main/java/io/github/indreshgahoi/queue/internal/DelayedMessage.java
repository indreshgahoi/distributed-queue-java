package io.github.indreshgahoi.queue.internal;

import io.github.indreshgahoi.queue.Message;

import java.time.Instant;

public record DelayedMessage(
        Message message,
        int nextAttempt,
        Instant retryAt
) {
}