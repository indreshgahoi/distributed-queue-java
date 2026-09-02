package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;

import java.net.URI;
import java.time.Duration;

public interface NodeTopologyClient {
    NodeRegistration register(
            String nodeId,
            URI endpoint,
            Duration leaseDuration
    );

    NodeRegistration heartbeat(
            NodeRegistration registration,
            Duration leaseDuration
    );
}

