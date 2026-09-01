package io.github.indreshgahoi.queue.metadata.domain.exception;

public final class ProvisioningClaimLostException
        extends QueueMetadataException {
    public ProvisioningClaimLostException() {
        super("Provisioning claim is stale, expired, or no longer owned");
    }
}
