package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.out.ProvisioningMetadataClient;
import io.github.indreshgahoi.queue.node.application.port.out.QueueStorageProvisioner;
import io.github.indreshgahoi.queue.node.domain.model.ProvisioningAssignment;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProvisioningReconcilerTest {

    @Test
    void noClaimPerformsNoStorageWork() {
        FakeMetadata metadata = new FakeMetadata(Optional.empty());
        RecordingStorage storage = new RecordingStorage();
        ProvisioningReconciler reconciler = reconciler(metadata, storage);

        assertFalse(reconciler.runOnce());
        assertEquals(0, storage.calls);
        assertEquals(0, metadata.completions);
    }

    @Test
    void materializesStorageBeforeCompletingClaim() {
        ProvisioningAssignment assignment = assignment();
        FakeMetadata metadata = new FakeMetadata(
                Optional.of(assignment)
        );
        RecordingStorage storage = new RecordingStorage();
        ProvisioningReconciler reconciler = reconciler(metadata, storage);

        assertTrue(reconciler.runOnce());
        assertEquals(1, storage.calls);
        assertEquals(1, metadata.completions);
        assertEquals(0, metadata.failures);
    }

    @Test
    void storageFailureIsReportedAgainstSameClaim() {
        ProvisioningAssignment assignment = assignment();
        FakeMetadata metadata = new FakeMetadata(
                Optional.of(assignment)
        );
        QueueStorageProvisioner storage = ignored -> {
            throw new IllegalStateException("disk failed");
        };
        ProvisioningReconciler reconciler = reconciler(metadata, storage);

        assertThrows(ProvisioningException.class, reconciler::runOnce);
        assertEquals(0, metadata.completions);
        assertEquals(1, metadata.failures);
    }

    private ProvisioningReconciler reconciler(
            ProvisioningMetadataClient metadata,
            QueueStorageProvisioner storage
    ) {
        return new ProvisioningReconciler(
                "node-a",
                Duration.ofSeconds(30),
                () -> Optional.of(new NodeRegistration(
                        "node-a",
                        1,
                        Instant.parse("2026-09-01T12:01:00Z")
                )),
                metadata,
                storage
        );
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

    private static final class RecordingStorage
            implements QueueStorageProvisioner {
        private int calls;

        @Override
        public void provision(ProvisioningAssignment assignment) {
            calls++;
        }
    }

    private static final class FakeMetadata
            implements ProvisioningMetadataClient {
        private final Optional<ProvisioningAssignment> claim;
        private int completions;
        private int failures;

        private FakeMetadata(
                Optional<ProvisioningAssignment> claim
        ) {
            this.claim = claim;
        }

        @Override
        public Optional<ProvisioningAssignment> claim(
                String workerId,
                long registrationEpoch,
                Duration leaseDuration
        ) {
            return claim;
        }

        @Override
        public void complete(ProvisioningAssignment assignment) {
            completions++;
        }

        @Override
        public void fail(ProvisioningAssignment assignment) {
            failures++;
        }
    }
}
