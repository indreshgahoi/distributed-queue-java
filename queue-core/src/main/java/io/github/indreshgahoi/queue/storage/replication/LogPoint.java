package io.github.indreshgahoi.queue.storage.replication;

/**
 * Logical position used to prove that two logs share the same prefix.
 */
public record LogPoint(long logIndex, long logTerm) {
    public static final LogPoint EMPTY = new LogPoint(0, 0);

    public LogPoint {
        boolean empty = logIndex == 0 && logTerm == 0;
        boolean entry = logIndex > 0 && logTerm > 0;
        if (!empty && !entry) {
            throw new IllegalArgumentException(
                    "logIndex and logTerm must both be zero or both be positive"
            );
        }
    }
}
