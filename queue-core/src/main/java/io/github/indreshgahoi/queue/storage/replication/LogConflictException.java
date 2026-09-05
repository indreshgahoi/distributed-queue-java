package io.github.indreshgahoi.queue.storage.replication;

public final class LogConflictException extends ReplicaException {
    public LogConflictException(String message) {
        super(message);
    }
}
