package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.wal.WalRecord;

import java.util.Objects;

/**
 * Stable logical identity and queue mutation stored in one WAL frame.
 */
public record LogEntry(
        long logIndex,
        long logTerm,
        WalRecord record
) {
    public LogEntry {
        if (logIndex <= 0) {
            throw new IllegalArgumentException("logIndex must be positive");
        }
        if (logTerm <= 0) {
            throw new IllegalArgumentException("logTerm must be positive");
        }
        Objects.requireNonNull(record, "record");
    }

    public LogPoint point() {
        return new LogPoint(logIndex, logTerm);
    }
}
