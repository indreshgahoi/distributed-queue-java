package io.github.indreshgahoi.queue.storage;

import java.util.Objects;
import java.util.UUID;

public record StorageLineage(
        UUID queueId,
        UUID generationId,
        int partitionId
) {
    public StorageLineage {
        Objects.requireNonNull(queueId, "queueId");
        Objects.requireNonNull(generationId, "generationId");

        if (partitionId < 0) {
            throw new IllegalArgumentException(
                    "partitionId must not be negative"
            );
        }
    }

    public static StorageLineage create() {
        return new StorageLineage(
                UUID.randomUUID(),
                UUID.randomUUID(),
                0
        );
    }
}
