package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.in.ReconcileProvisioningUseCase;
import io.github.indreshgahoi.queue.node.application.port.out.ProvisioningMetadataClient;
import io.github.indreshgahoi.queue.node.application.port.out.QueueStorageProvisioner;
import io.github.indreshgahoi.queue.node.domain.model.ProvisioningAssignment;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public final class ProvisioningReconciler
        implements ReconcileProvisioningUseCase {
    private final String workerId;
    private final Duration leaseDuration;
    private final ProvisioningMetadataClient metadata;
    private final QueueStorageProvisioner storage;

    public ProvisioningReconciler(
            String workerId,
            Duration leaseDuration,
            ProvisioningMetadataClient metadata,
            QueueStorageProvisioner storage
    ) {
        this.workerId = Objects.requireNonNull(workerId, "workerId");
        this.leaseDuration = Objects.requireNonNull(
                leaseDuration,
                "leaseDuration"
        );
        this.metadata = Objects.requireNonNull(metadata, "metadata");
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    @Override
    public boolean runOnce() {
        Optional<ProvisioningAssignment> claimed = metadata.claim(
                workerId,
                leaseDuration
        );
        if (claimed.isEmpty()) {
            log.trace(
                    "event=provisioning_claim_empty workerId={}",
                    workerId
            );
            return false;
        }
        ProvisioningAssignment assignment = claimed.orElseThrow();
        log.info(
                "event=provisioning_claim_acquired queueId={} "
                        + "generationId={} partitionId={} workerId={} "
                        + "fencingToken={} leaseExpiresAt={}",
                assignment.queueId(),
                assignment.generationId(),
                assignment.partitionId(),
                assignment.workerId(),
                assignment.fencingToken(),
                assignment.leaseExpiresAt()
        );
        try {
            storage.provision(assignment);
            metadata.complete(assignment);
            log.info(
                    "event=provisioning_completed queueId={} "
                            + "generationId={} partitionId={} workerId={} "
                            + "fencingToken={}",
                    assignment.queueId(),
                    assignment.generationId(),
                    assignment.partitionId(),
                    assignment.workerId(),
                    assignment.fencingToken()
            );
            return true;
        } catch (RuntimeException failure) {
            try {
                metadata.fail(assignment);
            } catch (RuntimeException reportingFailure) {
                failure.addSuppressed(reportingFailure);
            }
            throw new ProvisioningException(
                    "Failed to provision queue " + assignment.queueId(),
                    failure
            );
        }
    }
}
