package io.github.indreshgahoi.queue.storage;

public record WalPosition(
        long segmentId,
        long offset
) {
    public WalPosition {
        if (segmentId < 0) {
            throw new IllegalArgumentException(
                    "segmentId must not be negative"
            );
        }

        if (offset < 0) {
            throw new IllegalArgumentException(
                    "offset must not be negative"
            );
        }
    }
}