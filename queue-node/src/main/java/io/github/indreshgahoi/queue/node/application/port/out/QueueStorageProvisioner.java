package io.github.indreshgahoi.queue.node.application.port.out;

import io.github.indreshgahoi.queue.node.domain.model.ProvisioningAssignment;

public interface QueueStorageProvisioner {
    void provision(ProvisioningAssignment assignment);
}
