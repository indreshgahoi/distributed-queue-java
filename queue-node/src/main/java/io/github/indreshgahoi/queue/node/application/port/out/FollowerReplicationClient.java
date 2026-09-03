package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatch;
import io.github.indreshgahoi.queue.node.domain.model.ReplicaWalBatchResult;

import java.net.URI;

public interface FollowerReplicationClient {

    ReplicaWalBatchResult replicate(
            URI followerEndpoint,
            ReplicaWalBatch batch
    );
}
