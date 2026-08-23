package io.github.indreshgahoi.queue.storage.snapshot;

public record ReadySnapshotEntry(
        String messageId,
        String payload,
        int nextAttempt
) {
}