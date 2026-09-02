package io.github.indreshgahoi.queue.gateway.domain.model;

import java.net.URI;

public record ForwardResponse(
        int statusCode,
        String contentType,
        URI location,
        String body
) {
    public ForwardResponse {
        if (statusCode < 100 || statusCode > 599) {
            throw new IllegalArgumentException("Invalid HTTP status code");
        }
    }
}
