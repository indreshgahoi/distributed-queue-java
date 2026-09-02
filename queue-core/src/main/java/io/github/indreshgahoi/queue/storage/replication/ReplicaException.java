package io.github.indreshgahoi.queue.storage.replication;

public class ReplicaException extends RuntimeException {

    public ReplicaException(String message) {
        super(message);
    }

    public ReplicaException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
