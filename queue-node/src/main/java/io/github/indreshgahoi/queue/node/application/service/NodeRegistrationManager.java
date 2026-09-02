package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.out.NodeRegistrationProvider;
import io.github.indreshgahoi.queue.node.application.port.out.NodeTopologyClient;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public final class NodeRegistrationManager
        implements NodeRegistrationProvider {
    private final String nodeId;
    private final URI endpoint;
    private final Duration leaseDuration;
    private final NodeTopologyClient topology;
    private volatile NodeRegistration current;

    public NodeRegistrationManager(
            String nodeId,
            URI endpoint,
            Duration leaseDuration,
            NodeTopologyClient topology
    ) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.leaseDuration = Objects.requireNonNull(
                leaseDuration,
                "leaseDuration"
        );
        this.topology = Objects.requireNonNull(topology, "topology");
    }

    public synchronized void maintainLease() {
        NodeRegistration previous = current;
        try {
            current = previous == null
                    ? topology.register(nodeId, endpoint, leaseDuration)
                    : topology.heartbeat(previous, leaseDuration);
            if (previous == null
                    || previous.registrationEpoch()
                    != current.registrationEpoch()) {
                log.info(
                        "event=queue_node_registration_acquired "
                                + "nodeId={} registrationEpoch={} "
                                + "leaseExpiresAt={}",
                        current.nodeId(),
                        current.registrationEpoch(),
                        current.leaseExpiresAt()
                );
            }
        } catch (RuntimeException failure) {
            current = null;
            throw failure;
        }
    }

    @Override
    public Optional<NodeRegistration> currentRegistration() {
        return Optional.ofNullable(current);
    }
}

