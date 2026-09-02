package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.net.URI;

record RegisterNodeRequest(
        @NotBlank @Size(max = 255) String nodeId,
        @NotNull URI endpoint,
        @Min(1) @Max(3600) long leaseSeconds
) {
}

