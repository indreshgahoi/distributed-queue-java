package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WalCompactionPlannerTest {

    private final WalCompactionPlanner planner =
            new WalCompactionPlanner();

    @Test
    void snapshotInFirstSegmentReclaimsNothing() {
        WalCompactionPlan plan = planner.plan(
                new WalPosition(0, 10_000)
        );

        assertFalse(plan.hasReclaimableSegments());
    }

    @Test
    void snapshotAuthorizesOnlySegmentsStrictlyBeforeItsSegment() {
        WalCompactionPlan plan = planner.plan(
                new WalPosition(3, 0)
        );

        assertTrue(plan.hasReclaimableSegments());
        assertEquals(3, plan.boundarySegmentId());
    }

    @Test
    void snapshotAtEndOfSegmentStillRetainsBoundarySegment() {
        WalCompactionPlan plan = planner.plan(
                new WalPosition(2, Long.MAX_VALUE)
        );

        assertEquals(2, plan.boundarySegmentId());
    }
}
