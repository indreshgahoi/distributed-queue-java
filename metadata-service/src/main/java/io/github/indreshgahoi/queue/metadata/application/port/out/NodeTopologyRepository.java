package io.github.indreshgahoi.queue.metadata.application.port.out;

import io.github.indreshgahoi.queue.metadata.domain.model.NodeLeaseIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.metadata.domain.model.RegisterNodeCommand;

import java.time.Duration;
import java.util.List;

public interface NodeTopologyRepository {
    NodeRegistration register(RegisterNodeCommand command);

    NodeRegistration heartbeat(
            NodeLeaseIdentity identity,
            Duration leaseDuration
    );

    List<NodeRegistration> nodes();

    List<PartitionPlacement> placements();
}
