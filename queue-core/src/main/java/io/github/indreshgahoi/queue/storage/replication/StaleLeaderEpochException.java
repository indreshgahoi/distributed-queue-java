package io.github.indreshgahoi.queue.storage.replication;

public final class StaleLeaderEpochException
        extends ReplicaException {

    private final long supplied;
    private final long current;

    public StaleLeaderEpochException(
            long supplied,
            long current
    ) {
        super(
                "Stale leader epoch " + supplied
                        + "; follower has observed " + current
        );
        this.supplied = supplied;
        this.current = current;
    }

    public long supplied() {
        return supplied;
    }

    public long current() {
        return current;
    }
}
