package io.github.indreshgahoi.queue.metadata.application.port.out;

import io.github.indreshgahoi.queue.metadata.domain.model.CreateQueueCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.ClaimProvisioningCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaim;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaimIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueLifecycleState;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface QueueMetadataRepository {

    QueueDescriptor create(CreateQueueCommand command);

    Optional<ProvisioningClaim> claimProvisioning(
            ClaimProvisioningCommand command
    );

    QueueDescriptor completeProvisioning(
            ProvisioningClaimIdentity claim
    );

    QueueDescriptor failProvisioning(
            ProvisioningClaimIdentity claim
    );

    Optional<QueueDescriptor> find(
            String tenantId,
            String queueName
    );

    List<QueueDescriptor> list(String tenantId);

    QueueDescriptor beginDeletion(
            String tenantId,
            String queueName
    );

    QueueDescriptor transition(
            UUID queueId,
            UUID generationId,
            long expectedVersion,
            QueueLifecycleState expectedState,
            QueueLifecycleState nextState
    );
}
