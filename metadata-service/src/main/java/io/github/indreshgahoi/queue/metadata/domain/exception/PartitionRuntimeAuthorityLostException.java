package io.github.indreshgahoi.queue.metadata.domain.exception;

public final class PartitionRuntimeAuthorityLostException
        extends QueueMetadataException {
    public PartitionRuntimeAuthorityLostException() {
        super("Partition runtime authority is stale or expired");
    }
}
