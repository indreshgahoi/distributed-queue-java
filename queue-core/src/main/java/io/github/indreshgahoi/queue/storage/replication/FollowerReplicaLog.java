package io.github.indreshgahoi.queue.storage.replication;

import io.github.indreshgahoi.queue.storage.StorageLineage;

public interface FollowerReplicaLog extends AutoCloseable {

    ReplicaAppendResult append(ReplicatedWalEntry entry);

    StorageLineage lineage();

    long lastSequence();

    long highestLeaderEpoch();

    @Override
    void close();
}
