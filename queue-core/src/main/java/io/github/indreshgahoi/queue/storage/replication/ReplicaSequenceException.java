package io.github.indreshgahoi.queue.storage.replication;

public final class ReplicaSequenceException
        extends ReplicaException {

    public ReplicaSequenceException(
            long supplied,
            long expected
    ) {
        super(
                "Out-of-order replica sequence " + supplied
                        + "; expected " + expected
        );
    }
}
