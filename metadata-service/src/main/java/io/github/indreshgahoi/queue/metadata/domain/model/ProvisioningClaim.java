package io.github.indreshgahoi.queue.metadata.domain.model;

import java.time.Instant;
import java.util.Objects;

public record ProvisioningClaim(
        QueueDescriptor queue,
        ProvisioningClaimIdentity identity,
        Instant leaseExpiresAt
) {
    public ProvisioningClaim {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(identity, "identity");
        Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
        if (!queue.queueId().equals(identity.queueId())
                || !queue.generationId().equals(identity.generationId())) {
            throw new IllegalArgumentException(
                    "Claim identity must match queue lineage"
            );
        }
    }
}
