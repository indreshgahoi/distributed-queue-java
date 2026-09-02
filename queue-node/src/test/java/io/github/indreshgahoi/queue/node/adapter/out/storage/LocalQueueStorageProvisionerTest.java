package io.github.indreshgahoi.queue.node.adapter.out.storage;

import io.github.indreshgahoi.queue.node.config.QueueNodeProperties;
import io.github.indreshgahoi.queue.node.domain.model.ProvisioningAssignment;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalQueueStorageProvisionerTest {
    private static final long SEGMENT_BYTES = 1024;

    @TempDir
    private Path storageRoot;

    @Test
    void repeatedProvisioningReopensTheSameLineage() {
        ProvisioningAssignment assignment = assignment();
        LocalQueueStorageProvisioner provisioner = provisioner();

        provisioner.provision(assignment);

        assertDoesNotThrow(() -> provisioner.provision(assignment));
    }

    @Test
    void foreignLineageAtTargetPathFailsClosed() {
        ProvisioningAssignment assignment = assignment();
        Path walDirectory = walDirectory(assignment);
        StorageLineage foreign = new StorageLineage(
                UUID.randomUUID(),
                UUID.randomUUID(),
                0
        );
        try (SegmentedFileWriteAheadLog ignored =
                     new SegmentedFileWriteAheadLog(
                             walDirectory,
                             SEGMENT_BYTES,
                             foreign
                     )) {
            // Establish foreign authority at the assigned path.
        }

        assertThrows(
                WalException.class,
                () -> provisioner().provision(assignment)
        );
    }

    private LocalQueueStorageProvisioner provisioner() {
        return new LocalQueueStorageProvisioner(
                new QueueNodeProperties(
                        "node-a",
                        URI.create("http://node-a:8081"),
                        URI.create("http://localhost:8080"),
                        storageRoot,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(30),
                        SEGMENT_BYTES,
                        1024,
                        100,
                        1024 * 1024
                )
        );
    }

    private Path walDirectory(
            ProvisioningAssignment assignment
    ) {
        return storageRoot
                .resolve(assignment.queueId().toString())
                .resolve(assignment.generationId().toString())
                .resolve("partition-0")
                .resolve("wal");
    }

    private ProvisioningAssignment assignment() {
        return new ProvisioningAssignment(
                "tenant-a",
                "orders",
                UUID.randomUUID(),
                UUID.randomUUID(),
                0,
                "node-a",
                1,
                1,
                1,
                Instant.parse("2026-09-01T12:00:30Z")
        );
    }
}
