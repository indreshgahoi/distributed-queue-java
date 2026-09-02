package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import io.github.indreshgahoi.queue.metadata.domain.model.NodeRegistration;

import java.net.URI;
import java.time.Instant;

record NodeRegistrationResponse(
        String nodeId,
        URI endpoint,
        long registrationEpoch,
        Instant leaseExpiresAt,
        Instant registeredAt,
        Instant updatedAt
) {
    static NodeRegistrationResponse from(NodeRegistration registration) {
        return new NodeRegistrationResponse(
                registration.nodeId(),
                registration.endpoint(),
                registration.registrationEpoch(),
                registration.leaseExpiresAt(),
                registration.registeredAt(),
                registration.updatedAt()
        );
    }
}

