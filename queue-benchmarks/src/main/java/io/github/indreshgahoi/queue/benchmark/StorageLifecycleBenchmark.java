package io.github.indreshgahoi.queue.benchmark;

import io.github.indreshgahoi.queue.LocalMessageQueue;
import io.github.indreshgahoi.queue.QueueConfiguration;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.snapshot.FileQueueSnapshotStore;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WalRecordType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class StorageLifecycleBenchmark {

    @Benchmark
    public void frequentSegmentRotation(
            RotationState state,
            Blackhole blackhole
    ) {
        state.wal.append(state.record);
        blackhole.consume(state.wal.currentDurablePosition());
    }

    @Benchmark
    @Group("snapshotCreation")
    @GroupThreads(1)
    public void foregroundPublishDuringSnapshot(
            SnapshotState state,
            Blackhole blackhole
    ) {
        blackhole.consume(state.queue.publish("foreground"));
    }

    @Benchmark
    @Group("snapshotCreation")
    @GroupThreads(1)
    public void createSnapshot(
            SnapshotState state
    ) {
        state.snapshots.save(state.queue.captureSnapshot());
    }

    @State(Scope.Thread)
    public static class RotationState {
        private Path directory;
        private SegmentedFileWriteAheadLog wal;
        private WalRecord record;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("wal-rotation-jmh-");
            // A small target deliberately makes rotation a frequent part of
            // the measured append population instead of a rare outlier.
            wal = new SegmentedFileWriteAheadLog(
                    directory.resolve("wal"),
                    4 * 1024,
                    StorageLineage.create()
            );
            record = new WalRecord(
                    WalRecordType.PUBLISH,
                    "rotation-message",
                    "x".repeat(1024),
                    null,
                    1,
                    Instant.parse("2026-09-03T00:00:00Z")
            );
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            wal.close();
            BenchmarkFiles.deleteTree(directory);
        }
    }

    @State(Scope.Group)
    public static class SnapshotState {
        private Path directory;
        private LocalMessageQueue queue;
        private FileQueueSnapshotStore snapshots;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("snapshot-contention-jmh-");
            snapshots = new FileQueueSnapshotStore(
                    directory.resolve("snapshot.bin")
            );
            queue = new LocalMessageQueue(
                    Clock.systemUTC(),
                    new QueueConfiguration(),
                    new SegmentedFileWriteAheadLog(
                            directory.resolve("wal"),
                            512L * 1024 * 1024,
                            StorageLineage.create()
                    ),
                    snapshots
            );
            for (int index = 0; index < 100; index++) {
                queue.publish("x".repeat(1024));
            }
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            queue.close();
            BenchmarkFiles.deleteTree(directory);
        }
    }
}
