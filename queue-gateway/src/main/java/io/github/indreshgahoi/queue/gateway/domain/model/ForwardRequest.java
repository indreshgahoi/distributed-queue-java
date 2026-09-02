package io.github.indreshgahoi.queue.gateway.domain.model;

import java.util.Objects;

public record ForwardRequest(
        Method method,
        String path,
        String body,
        String contentType
) {
    public ForwardRequest {
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(path, "path");
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("path must be absolute");
        }
    }

    public enum Method {
        POST
    }
}
