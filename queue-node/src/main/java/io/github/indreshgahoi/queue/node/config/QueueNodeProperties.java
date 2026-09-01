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
        @NotNull URI metadataBaseUrl,
        @NotNull Path storageRoot,
        @NotNull Duration leaseDuration,
        @Positive long walSegmentBytes
) {
}
