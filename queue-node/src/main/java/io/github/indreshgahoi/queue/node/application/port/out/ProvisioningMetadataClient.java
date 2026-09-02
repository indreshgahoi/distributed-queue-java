package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.node.domain.model.ProvisioningAssignment;

import java.time.Duration;
import java.util.Optional;

public interface ProvisioningMetadataClient {
    Optional<ProvisioningAssignment> claim(
            String workerId,
            long registrationEpoch,
            Duration leaseDuration
    );

    void complete(ProvisioningAssignment assignment);

    void fail(ProvisioningAssignment assignment);
}
