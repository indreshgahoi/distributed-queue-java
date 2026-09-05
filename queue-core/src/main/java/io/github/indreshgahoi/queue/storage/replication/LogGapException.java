package io.github.indreshgahoi.queue.storage.replication;

public final class LogGapException extends ReplicaException {
    public LogGapException(long requested, long expected) {
        super("Log gap: requested index " + requested + ", expected " + expected);
    }
}
