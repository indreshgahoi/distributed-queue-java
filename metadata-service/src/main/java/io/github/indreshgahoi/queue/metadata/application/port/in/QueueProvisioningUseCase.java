package io.github.indreshgahoi.queue.metadata.application.port.in;

import io.github.indreshgahoi.queue.metadata.domain.model.ClaimProvisioningCommand;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaim;
import io.github.indreshgahoi.queue.metadata.domain.model.ProvisioningClaimIdentity;
import io.github.indreshgahoi.queue.metadata.domain.model.QueueDescriptor;

import java.util.Optional;

public interface QueueProvisioningUseCase {

    Optional<ProvisioningClaim> claim(
            ClaimProvisioningCommand command
    );

    QueueDescriptor complete(
            ProvisioningClaimIdentity claim
    );

    QueueDescriptor fail(
            ProvisioningClaimIdentity claim
    );
}
