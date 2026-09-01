package io.github.indreshgahoi.queue.storage.lifecycle;

import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.compaction.SnapshotCompactionCoordinator;
import io.github.indreshgahoi.queue.storage.snapshot.QueueSnapshot;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class StorageLifecycleManager
        implements AutoCloseable {

    private final Supplier<WalPosition> walPositionSupplier;
    private final Supplier<QueueSnapshot> snapshotSupplier;
    private final SnapshotCompactionCoordinator checkpointCoordinator;
    private final CheckpointPolicy checkpointPolicy;
    private final Duration checkInterval;
    private final ScheduledExecutorService scheduler;

    private Optional<WalPosition> latestSnapshotPosition;
    private boolean compactionRetryRequired;
    private RuntimeException lastFailure;
    private boolean started;
    private boolean closed;

    public StorageLifecycleManager(
            Supplier<WalPosition> walPositionSupplier,
            Supplier<QueueSnapshot> snapshotSupplier,
            SnapshotCompactionCoordinator checkpointCoordinator,
            CheckpointPolicy checkpointPolicy,
            Duration checkInterval
    ) {
        this(
                walPositionSupplier,
                snapshotSupplier,
                checkpointCoordinator,
                checkpointPolicy,
                checkInterval,
                Executors.newSingleThreadScheduledExecutor(
                        lifecycleThreadFactory()
                )
        );
    }

    StorageLifecycleManager(
            Supplier<WalPosition> walPositionSupplier,
            Supplier<QueueSnapshot> snapshotSupplier,
            SnapshotCompactionCoordinator checkpointCoordinator,
            CheckpointPolicy checkpointPolicy,
            Duration checkInterval,
            ScheduledExecutorService scheduler
    ) {
        this.walPositionSupplier = Objects.requireNonNull(
                walPositionSupplier,
                "walPositionSupplier"
        );
        this.snapshotSupplier = Objects.requireNonNull(
                snapshotSupplier,
                "snapshotSupplier"
        );
        this.checkpointCoordinator = Objects.requireNonNull(
                checkpointCoordinator,
                "checkpointCoordinator"
        );
        this.checkpointPolicy = Objects.requireNonNull(
                checkpointPolicy,
                "checkpointPolicy"
        );
        this.checkInterval = Objects.requireNonNull(
                checkInterval,
                "checkInterval"
        );
        this.scheduler = Objects.requireNonNull(
                scheduler,
                "scheduler"
        );

        if (checkInterval.isZero()
                || checkInterval.isNegative()) {
            throw new IllegalArgumentException(
                    "checkInterval must be greater than zero"
            );
        }

        latestSnapshotPosition =
                checkpointCoordinator.latestSnapshot()
                        .map(QueueSnapshot::walPosition);

        compactionRetryRequired =
                latestSnapshotPosition.isPresent();
    }

    public synchronized void start() {
        ensureOpen();

        if (started) {
            return;
        }

        started = true;
        scheduler.scheduleWithFixedDelay(
                this::runScheduledCycle,
                0,
                checkInterval.toNanos(),
                TimeUnit.NANOSECONDS
        );
    }

    public synchronized boolean runOnce() {
        ensureOpen();

        try {
            if (compactionRetryRequired) {
                latestSnapshotPosition =
                        checkpointCoordinator
                                .compactLatestSnapshot();
                compactionRetryRequired = false;
                lastFailure = null;
                return latestSnapshotPosition.isPresent();
            }

            WalPosition currentPosition =
                    walPositionSupplier.get();

            if (!checkpointPolicy.shouldCheckpoint(
                    latestSnapshotPosition,
                    currentPosition
            )) {
                lastFailure = null;
                return false;
            }

            QueueSnapshot snapshot =
                    snapshotSupplier.get();

            try {
                checkpointCoordinator.commitSnapshot(snapshot);
                latestSnapshotPosition =
                        Optional.of(snapshot.walPosition());
                lastFailure = null;
                return true;

            } catch (RuntimeException failure) {
                reconcileAfterFailedCommit(snapshot);
                throw failure;
            }

        } catch (RuntimeException failure) {
            lastFailure = failure;
            throw new StorageLifecycleException(
                    "Storage maintenance cycle failed",
                    failure
            );
        }
    }

    public synchronized Optional<RuntimeException> lastFailure() {
        return Optional.ofNullable(lastFailure);
    }

    public synchronized Optional<WalPosition> latestSnapshotPosition() {
        return latestSnapshotPosition;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;
        scheduler.shutdown();
    }

    private void runScheduledCycle() {
        try {
            runOnce();
        } catch (StorageLifecycleException ignored) {
            /*
             * Failure remains observable through lastFailure(). A fixed-delay
             * scheduler must continue so transient failures can be retried.
             */
        }
    }

    private void reconcileAfterFailedCommit(
            QueueSnapshot candidate
    ) {
        Optional<WalPosition> authoritative =
                checkpointCoordinator.latestSnapshot()
                        .map(QueueSnapshot::walPosition);

        latestSnapshotPosition = authoritative;
        compactionRetryRequired =
                authoritative.isPresent()
                        && authoritative.orElseThrow()
                        .equals(candidate.walPosition());
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(
                    "Storage lifecycle manager is closed"
            );
        }
    }

    private static ThreadFactory lifecycleThreadFactory() {
        return runnable -> {
            Thread thread =
                    new Thread(
                            runnable,
                            "queue-storage-lifecycle"
                    );
            thread.setDaemon(true);
            return thread;
        };
    }
}
