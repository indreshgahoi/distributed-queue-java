package io.github.indreshgahoi.queue.storage.replication;

public final class StaleLeaderEpochException
        extends ReplicaException {

    public StaleLeaderEpochException(
            long supplied,
            long current
    ) {
        super(
                "Stale leader epoch " + supplied
                        + "; follower has observed " + current
        );
    }
}
