package io.github.indreshgahoi.queue.metadata.domain.exception;

public final class NodeLeaseLostException
        extends QueueMetadataException {
    public NodeLeaseLostException() {
        super("Queue-node registration lease is no longer authoritative");
    }
}

