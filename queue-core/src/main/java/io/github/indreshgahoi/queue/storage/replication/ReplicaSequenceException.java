package io.github.indreshgahoi.queue.storage.replication;

public final class ReplicaSequenceException
        extends ReplicaException {

    private final long supplied;
    private final long expected;

    public ReplicaSequenceException(
            long supplied,
            long expected
    ) {
        super(
                "Out-of-order replica sequence " + supplied
                        + "; expected " + expected
        );
        this.supplied = supplied;
        this.expected = expected;
    }

    public long supplied() {
        return supplied;
    }

    public long expected() {
        return expected;
    }
}
