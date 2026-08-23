package io.github.indreshgahoi.queue.storage.snapshot;

import java.time.Instant;

public record InFlightSnapshotEntry(
        String messageId,
        String payload,
        String receiptHandle,
        int attempt,
        Instant leaseUntil
) {
}