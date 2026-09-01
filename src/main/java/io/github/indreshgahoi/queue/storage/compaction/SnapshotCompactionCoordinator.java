package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshotStore;

import java.util.Objects;
import java.util.Optional;

public final class SnapshotCompactionCoordinator {

    private final QueueSnapshotStore snapshotStore;
    private final WalCompactionBoundaryTracker boundaryTracker;
    private final WalCompactor walCompactor;
    private final SnapshotPositionValidator positionValidator;

    public SnapshotCompactionCoordinator(
            QueueSnapshotStore snapshotStore,
            WalCompactionBoundaryTracker boundaryTracker,
            SnapshotPositionValidator positionValidator
    ) {
        this(
                snapshotStore,
                boundaryTracker,
                positionValidator,
                new NoOpWalCompactor()
        );
    }

    public SnapshotCompactionCoordinator(
            QueueSnapshotStore snapshotStore,
            WalCompactionBoundaryTracker boundaryTracker,
            SnapshotPositionValidator positionValidator,
            WalCompactor walCompactor
    ) {
        this.snapshotStore =
                Objects.requireNonNull(
                        snapshotStore,
                        "snapshotStore"
                );

        this.boundaryTracker =
                Objects.requireNonNull(
                        boundaryTracker,
                        "boundaryTracker"
                );
        this.walCompactor =
                Objects.requireNonNull(
                        walCompactor,
                        "walCompactor"
                );

        this.positionValidator =
                Objects.requireNonNull(
                        positionValidator,
                        "positionValidator"
                );

        initializeBoundaryFromAuthoritativeSnapshot();
    }

    public synchronized void commitSnapshot(
            QueueSnapshot snapshot
    ) {
        Objects.requireNonNull(
                snapshot,
                "snapshot"
        );

        WalPosition candidate =
                snapshot.walPosition();

        /*
         * Reject stale snapshots BEFORE they can
         * replace the authoritative snapshot.
         */
        Optional<WalPosition> current =
                boundaryTracker.currentBoundary();

        if (current.isPresent()
                && candidate.compareTo(current.get()) < 0
        ) {

            throw new IllegalArgumentException(
                    "Snapshot position "
                            + candidate
                            + " is older than current compaction boundary "
                            + current.get()
            );
        }

        positionValidator.validate(
                candidate
        );

        /*
         * Only after validation may the snapshot
         * become authoritative.
         */
        snapshotStore.save(snapshot);

        /*
         * Successful snapshot promotion allows
         * the boundary to advance.
         */
        compactValidatedPosition(
                candidate
        );
    }

    public synchronized Optional<WalPosition> compactLatestSnapshot() {
        Optional<QueueSnapshot> latest =
                latestSnapshot();

        if (latest.isEmpty()) {
            return Optional.empty();
        }

        WalPosition position =
                latest.orElseThrow()
                        .walPosition();

        positionValidator.validate(
                position
        );

        compactValidatedPosition(
                position
        );

        return Optional.of(
                position
        );
    }

    public synchronized Optional<QueueSnapshot> latestSnapshot() {
        return snapshotStore
                .loadLatest();
    }

    private void initializeBoundaryFromAuthoritativeSnapshot() {
        latestSnapshot()
                .ifPresent(
                        this::initializeBoundary
                );
    }

    private void initializeBoundary(
            QueueSnapshot snapshot
    ) {
        WalPosition position =
                snapshot.walPosition();

        positionValidator.validate(
                position
        );

        boundaryTracker.advanceTo(
                position
        );
    }

    private void compactValidatedPosition(
            WalPosition position
    ) {
        /*
         * Only a validated, successfully promoted snapshot may authorize
         * deletion. Reclamation failure does not invalidate that recovery
         * point and remains safe to retry.
         */
        boundaryTracker.advanceTo(
                position
        );

        walCompactor.compactThrough(
                position
        );
    }
}
