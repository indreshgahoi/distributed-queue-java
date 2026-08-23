package io.github.indreshgahoi.queue.storage.snapshot;

import java.time.Instant;

public record DelayedSnapshotEntry(
        String messageId,
        String payload,
        int nextAttempt,
        Instant retryAt
) {
}