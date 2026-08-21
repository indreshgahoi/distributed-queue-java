package io.github.indreshgahoi.queue;

import java.time.Instant;

record WalRecord(
        WalRecordType type,
        String messageId,
        String payload,
        String receiptHandle,
        int attempt,
        Instant timestamp
) {
}