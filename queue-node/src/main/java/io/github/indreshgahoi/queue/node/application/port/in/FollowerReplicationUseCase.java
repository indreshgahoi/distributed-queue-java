package io.github.indreshgahoi.queue.node.application.port.in;

import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatch;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatchResult;

public interface FollowerReplicationUseCase {

    ReplicaWalBatchResult replicate(ReplicaWalBatch batch);
}
