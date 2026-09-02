package io.github.indreshgahoi.queue.gateway.domain.exception;

public final class RoutingMetadataUnavailableException
        extends RuntimeException {
    public RoutingMetadataUnavailableException(Throwable cause) {
        super("Routing metadata is unavailable", cause);
    }
}
