package io.github.indreshgahoi.queue.node.application.service;

public final class ProvisioningException extends RuntimeException {
    public ProvisioningException(
            String message,
            Throwable cause
    ) {
        super(message, cause);
    }
}
