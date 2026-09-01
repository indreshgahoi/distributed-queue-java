package io.github.indreshgahoi.queue.metadata.domain.exception;

public final class StaleQueueMetadataException
        extends QueueMetadataException {

    public StaleQueueMetadataException() {
        super("Queue metadata transition used stale authority");
    }
}
