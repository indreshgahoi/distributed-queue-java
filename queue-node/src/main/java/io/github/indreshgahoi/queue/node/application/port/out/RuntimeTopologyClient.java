package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionState;

import java.util.List;

public interface RuntimeTopologyClient {
    List<PartitionPlacement> activePlacements(
            NodeRegistration registration
    );

    void publishStatus(
            RuntimePartitionIdentity identity,
            RuntimePartitionState state,
            String failureReason
    );
}
