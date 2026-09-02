package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.StorageLineage;

public final class ReplicaLineageMismatchException
        extends ReplicaException {

    public ReplicaLineageMismatchException(
            StorageLineage supplied,
            StorageLineage expected
    ) {
        super(
                "Replica lineage " + supplied
                        + " does not match follower lineage " + expected
        );
    }
}
