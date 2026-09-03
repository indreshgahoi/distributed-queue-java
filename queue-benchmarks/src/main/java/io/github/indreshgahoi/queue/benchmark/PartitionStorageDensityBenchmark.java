package io.github.indreshgahoi.queue.benchmark;

import io.github.indreshgahoi.queue.storage.StorageLineage;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PartitionStorageDensityBenchmark {

    @Benchmark
    public void openIdlePartitions(
            DensityState state,
            Blackhole blackhole
    ) throws IOException {
        blackhole.consume(state.openPartitions(false));
    }

    @Benchmark
    public void openAndActivatePartitions(
            DensityState state,
            Blackhole blackhole
    ) throws IOException {
        blackhole.consume(state.openPartitions(true));
    }

    @State(Scope.Thread)
    public static class DensityState {
        @Param({"1", "32", "128"})
        private int partitionCount;
        private Path root;
        private List<SegmentedFileWriteAheadLog> logs;

        @Setup(Level.Invocation)
        public void setUp() throws IOException {
            root = Files.createTempDirectory("partition-density-jmh-");
            logs = new ArrayList<>();
        }

        public int openPartitions(boolean activate) throws IOException {
            for (int index = 0; index < partitionCount; index++) {
                SegmentedFileWriteAheadLog wal =
                        new SegmentedFileWriteAheadLog(
                                root.resolve("partition-" + index),
                                16L * 1024 * 1024,
                                StorageLineage.create()
                        );
                logs.add(wal);
                if (activate) {
                    wal.append(record(index));
                }
            }
            return logs.size();
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws IOException {
            for (SegmentedFileWriteAheadLog wal : logs) {
                wal.close();
            }
            BenchmarkFiles.deleteTree(root);
        }

        private static WalRecord record(int index) {
            return new WalRecord(
                    WalRecordType.PUBLISH,
                    "partition-message-" + index,
                    "x".repeat(1024),
                    null,
                    1,
                    Instant.parse("2026-09-03T00:00:00Z")
            );
        }
    }
}
