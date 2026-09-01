package io.github.indreshgahoi.queue.storage.lifecycle;

import io.github.indreshgahoi.queue.storage.WalPosition;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmentDistanceCheckpointPolicyTest {

    @Test
    void rejectsNonPositiveSegmentDistance() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentDistanceCheckpointPolicy(0)
        );
    }

    @Test
    void noSnapshotUsesInitialSegmentAsBaseline() {
        SegmentDistanceCheckpointPolicy policy =
                new SegmentDistanceCheckpointPolicy(2);

        assertFalse(
                policy.shouldCheckpoint(
                        Optional.empty(),
                        new WalPosition(1, 100)
                )
        );
        assertTrue(
                policy.shouldCheckpoint(
                        Optional.empty(),
                        new WalPosition(2, 8)
                )
        );
    }

    @Test
    void latestSnapshotSegmentDefinesNextBaseline() {
        SegmentDistanceCheckpointPolicy policy =
                new SegmentDistanceCheckpointPolicy(3);
        Optional<WalPosition> snapshot =
                Optional.of(new WalPosition(5, 400));

        assertFalse(
                policy.shouldCheckpoint(
                        snapshot,
                        new WalPosition(7, 900)
                )
        );
        assertTrue(
                policy.shouldCheckpoint(
                        snapshot,
                        new WalPosition(8, 8)
                )
        );
    }
}
