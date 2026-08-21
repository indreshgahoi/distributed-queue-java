package io.github.indreshgahoi.queue;

import java.time.Instant;

record DelayedMessage(
        Message message,
        int nextAttempt,
        Instant retryAt
) {
}