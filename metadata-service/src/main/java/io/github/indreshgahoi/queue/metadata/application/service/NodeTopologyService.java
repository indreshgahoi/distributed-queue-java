package io.github.indreshgahoi.queue.metadata.application.service;

import io.github.indreshgahoi.queue.metadata.application.port.in.NodeTopologyUseCase;
import io.github.indreshgahoi.queue.metadata.application.port.out.NodeTopologyRepository;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeLeaseIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.metadata.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.metadata.domain.model.RegisterNodeCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

@Service
@Slf4j
final class NodeTopologyService implements NodeTopologyUseCase {
    private final NodeTopologyRepository repository;

    NodeTopologyService(NodeTopologyRepository repository) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override
    public NodeRegistration register(RegisterNodeCommand command) {
        NodeRegistration registration = repository.register(command);
        log.info(
                "event=queue_node_registered nodeId={} endpoint={} "
                        + "registrationEpoch={} leaseExpiresAt={}",
                registration.nodeId(),
                registration.endpoint(),
                registration.registrationEpoch(),
                registration.leaseExpiresAt()
        );
        return registration;
    }

    @Override
    public NodeRegistration heartbeat(
            NodeLeaseIdentity identity,
            Duration leaseDuration
    ) {
        return repository.heartbeat(identity, leaseDuration);
    }

    @Override
    public List<NodeRegistration> nodes() {
        return repository.nodes();
    }

    @Override
    public List<PartitionPlacement> placements() {
        return repository.placements();
    }
}
