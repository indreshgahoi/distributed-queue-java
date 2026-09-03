package io.github.indreshgahoi.queue.node.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "queue.node")
public record QueueNodeProperties(
        @NotBlank String id,
        @NotNull URI endpoint,
        @NotNull URI metadataBaseUrl,
        @NotNull Path storageRoot,
        @NotNull Duration registrationLeaseDuration,
        @NotNull Duration leaseDuration,
        @NotNull Duration httpConnectTimeout,
        @NotNull Duration httpRequestTimeout,
        @Positive long walSegmentBytes,
        @Positive int maxMessageBytes,
        @Positive int maxRetainedMessages,
        @Positive long maxRetainedBytes
) {
    public QueueNodeProperties {
        if (httpConnectTimeout.isZero()
                || httpConnectTimeout.isNegative()
                || httpRequestTimeout.isZero()
                || httpRequestTimeout.isNegative()) {
            throw new IllegalArgumentException(
                    "Queue-node HTTP timeouts must be positive"
            );
        }
    }
}
