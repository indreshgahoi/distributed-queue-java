package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record ClaimProvisioningRequest(
        @NotBlank @Size(max = 255) String workerId,
        @Min(1) @Max(3600) long leaseSeconds
) {
}
