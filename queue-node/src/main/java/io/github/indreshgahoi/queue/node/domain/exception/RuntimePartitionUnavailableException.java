package io.github.indreshgahoi.queue.node.domain.exception;

import java.util.UUID;

public final class RuntimePartitionUnavailableException
        extends RuntimeException {
    public RuntimePartitionUnavailableException(UUID queueId) {
        super("queue runtime is not READY on this node: " + queueId);
    }
}
