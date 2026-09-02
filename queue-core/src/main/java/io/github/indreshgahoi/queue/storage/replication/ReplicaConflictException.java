package io.github.indreshgahoi.queue.storage.replication;

public final class ReplicaConflictException
        extends ReplicaException {

    public ReplicaConflictException(long sequence) {
        super(
                "Replica sequence " + sequence
                        + " already contains a different WAL record"
        );
    }
}
