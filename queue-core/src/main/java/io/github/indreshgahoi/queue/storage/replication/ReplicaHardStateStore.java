package io.github.indreshgahoi.queue.storage.replication;

public interface ReplicaHardStateStore {
    ReplicaHardState load(long localDurableIndex);

    void save(ReplicaHardState state, long localDurableIndex);
}
