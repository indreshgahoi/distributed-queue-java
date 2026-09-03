package io.github.indreshgahoi.queue.benchmark;

import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.replication.OrderedFollowerReplicaLog;
import io.github.indreshgahoi.queue.storage.replication.ReplicatedWalEntry;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WalRecordType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class WalDurabilityBenchmark {

    @Benchmark
    public void forcedWalAppend(
            ForcedAppendState state,
            Blackhole blackhole
    ) {
        state.wal.append(state.record);
        blackhole.consume(state.wal.currentDurablePosition());
    }

    @Benchmark
    public void currentFollowerBatch(
            FollowerBatchState state,
            Blackhole blackhole
    ) {
        long firstSequence = state.nextSequence;
        List<ReplicatedWalEntry> entries = state.records.stream()
                .map(record -> new ReplicatedWalEntry(
                        state.lineage,
                        1,
                        state.nextSequence++,
                        record
                ))
                .toList();
        blackhole.consume(state.replica.appendBatch(entries));
        if (state.nextSequence != firstSequence + state.batchSize) {
            throw new IllegalStateException("benchmark sequence drift");
        }
    }

    @State(Scope.Thread)
    public static class ForcedAppendState {
        @Param({"128", "1024", "16384", "262144"})
        private int payloadBytes;

        private Path directory;
        private SegmentedFileWriteAheadLog wal;
        private WalRecord record;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("wal-force-jmh-");
            wal = new SegmentedFileWriteAheadLog(
                    directory.resolve("wal"),
                    512L * 1024 * 1024,
                    StorageLineage.create()
            );
            record = record(payloadBytes);
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            wal.close();
            BenchmarkFiles.deleteTree(directory);
        }
    }

    @State(Scope.Thread)
    public static class FollowerBatchState {
        @Param({"1", "8", "32", "128", "256"})
        private int batchSize;

        private Path directory;
        private StorageLineage lineage;
        private OrderedFollowerReplicaLog replica;
        private List<WalRecord> records;
        private long nextSequence;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("follower-batch-jmh-");
            lineage = StorageLineage.create();
            replica = new OrderedFollowerReplicaLog(
                    new SegmentedFileWriteAheadLog(
                            directory.resolve("wal"),
                            512L * 1024 * 1024,
                            lineage
                    ),
                    directory.resolve("leader-epoch.bin")
            );
            WalRecord record = record(1024);
            records = java.util.Collections.nCopies(batchSize, record);
            nextSequence = 1;
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            replica.close();
            BenchmarkFiles.deleteTree(directory);
        }
    }

    private static WalRecord record(int payloadBytes) {
        return new WalRecord(
                WalRecordType.PUBLISH,
                "benchmark-message",
                "x".repeat(payloadBytes),
                null,
                1,
                Instant.parse("2026-09-03T00:00:00Z")
        );
    }
}
