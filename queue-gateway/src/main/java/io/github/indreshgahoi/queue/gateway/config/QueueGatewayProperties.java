package io.github.indreshgahoi.queue.gateway.config;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "queue.gateway")
public record QueueGatewayProperties(
        @NotNull URI metadataBaseUrl,
        @NotNull Duration connectTimeout,
        @NotNull Duration requestTimeout
) {
    public QueueGatewayProperties {
        if (connectTimeout.isZero() || connectTimeout.isNegative()
                || requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException(
                    "Gateway HTTP timeouts must be positive"
            );
        }
    }
}
