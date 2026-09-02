package io.github.indreshgahoi.queue.metadata.domain.model;

import java.net.URI;
import java.time.Instant;
import java.util.Objects;

public record NodeRegistration(
        String nodeId,
        URI endpoint,
        long registrationEpoch,
        Instant leaseExpiresAt,
        Instant registeredAt,
        Instant updatedAt
) {
    public NodeRegistration {
        QueueDescriptor.requireText(nodeId, "nodeId");
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
        Objects.requireNonNull(registeredAt, "registeredAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if (registrationEpoch <= 0) {
            throw new IllegalArgumentException(
                    "registrationEpoch must be positive"
            );
        }
    }
}

