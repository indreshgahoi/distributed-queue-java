package io.github.indreshgahoi.queue.metadata.adapter.in.web;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

@Schema(description = "A request to create a queue in a tenant namespace")
public record CreateQueueRequest(
        @Schema(
                description = "Tenant-local queue name",
                example = "orders"
        )
        @NotBlank @Size(max = 255) String queueName
) {
}
