package io.github.indreshgahoi.queue.metadata.adapter.in.web.internal;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

record HeartbeatNodeRequest(
        @Min(1) long registrationEpoch,
        @Min(1) @Max(3600) long leaseSeconds
) {
}

