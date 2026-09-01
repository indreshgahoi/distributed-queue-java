package io.github.indreshgahoi.queue.metadata.domain.model;

import java.time.Duration;
import java.util.Objects;

public record ClaimProvisioningCommand(
        String workerId,
        Duration leaseDuration
) {
    public ClaimProvisioningCommand {
        QueueDescriptor.requireText(workerId, "workerId");
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isZero() || leaseDuration.isNegative()) {
            throw new IllegalArgumentException(
                    "leaseDuration must be positive"
            );
        }
    }
}
