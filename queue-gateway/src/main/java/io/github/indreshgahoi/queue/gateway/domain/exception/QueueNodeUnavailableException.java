package io.github.indreshgahoi.queue.gateway.domain.exception;

public final class QueueNodeUnavailableException extends RuntimeException {
    public QueueNodeUnavailableException(String nodeId, Throwable cause) {
        super("Queue node is unreachable: " + nodeId, cause);
    }
}
