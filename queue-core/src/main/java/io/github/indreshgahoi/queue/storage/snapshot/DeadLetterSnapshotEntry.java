package io.github.indreshgahoi.queue.storage.snapshot;

public record DeadLetterSnapshotEntry(
        String messageId,
        String payload
) {
}