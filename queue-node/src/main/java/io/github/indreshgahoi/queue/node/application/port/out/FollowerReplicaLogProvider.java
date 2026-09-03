package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.replication.FollowerReplicaLog;

public interface FollowerReplicaLogProvider {

    FollowerReplicaLog open(StorageLineage lineage);
}
