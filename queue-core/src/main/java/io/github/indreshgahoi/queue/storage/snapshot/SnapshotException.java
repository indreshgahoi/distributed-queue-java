package io.github.indreshgahoi.queue.storage.snapshot;

public final class SnapshotException
        extends RuntimeException {

    public SnapshotException(
            String message
    ) {
        super(message);
    }

    public SnapshotException(
            String message,
            Throwable cause
    ) {
        super(
                message,
                cause
        );
    }
}