package io.github.indreshgahoi.queue.storage.wal;

import java.time.Instant;

public record WalRecord(
        WalRecordType type,
        String messageId,
        String payload,
        String receiptHandle,
        int attempt,
        Instant timestamp
) {
}