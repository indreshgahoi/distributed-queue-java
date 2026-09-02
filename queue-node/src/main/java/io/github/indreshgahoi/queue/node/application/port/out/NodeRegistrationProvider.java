package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;

import java.util.Optional;

public interface NodeRegistrationProvider {
    Optional<NodeRegistration> currentRegistration();
}

