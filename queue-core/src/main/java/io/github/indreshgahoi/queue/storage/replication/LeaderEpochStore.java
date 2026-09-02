package io.github.indreshgahoi.queue.storage.replication;

interface LeaderEpochStore {

    long load();

    void save(long leaderEpoch);
}
