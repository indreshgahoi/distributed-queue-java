package io.github.indreshgahoi.queue.storage.compaction;

import io.github.indreshgahoi.queue.storage.StorageLineage;
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
    private final StorageLineage storageLineage;

    public SnapshotCompactionCoordinator(
            QueueSnapshotStore snapshotStore,
            WalCompactionBoundaryTracker boundaryTracker,
            StorageLineage storageLineage,
            SnapshotPositionValidator positionValidator
    ) {
        this(
                snapshotStore,
                boundaryTracker,
                storageLineage,
                positionValidator,
                new NoOpWalCompactor()
        );
    }

    public SnapshotCompactionCoordinator(
            QueueSnapshotStore snapshotStore,
            WalCompactionBoundaryTracker boundaryTracker,
            StorageLineage storageLineage,
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
        this.storageLineage =
                Objects.requireNonNull(
                        storageLineage,
                        "storageLineage"
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

        validateLineage(snapshot);

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

        validateLineage(latest.orElseThrow());

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
        validateLineage(snapshot);

        WalPosition position =
                snapshot.walPosition();

        positionValidator.validate(
                position
        );

        boundaryTracker.advanceTo(
                position
        );
    }

    private void validateLineage(
            QueueSnapshot snapshot
    ) {
        if (!storageLineage.equals(snapshot.storageLineage())) {
            throw new IllegalArgumentException(
                    "Snapshot lineage mismatch. Expected "
                            + storageLineage
                            + " but found "
                            + snapshot.storageLineage()
            );
        }
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
