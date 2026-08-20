package io.github.indreshgahoi.queue;

import java.time.Instant;

record InFlightMessage(
        Message message,
        String receiptHandle,
        Instant leaseUntil
) {
}