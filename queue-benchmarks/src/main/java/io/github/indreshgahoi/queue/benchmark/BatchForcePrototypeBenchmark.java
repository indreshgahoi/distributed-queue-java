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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

/**
 * Compares the previous force-per-record path, the production durability-group
 * path, and the original raw-file prototype that motivated the design.
 */
@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BatchForcePrototypeBenchmark {

    @Benchmark
    public void productionForcePerRecord(BenchmarkState state) {
        for (int index = 0; index < state.batchSize; index++) {
            state.productionWal.append(state.record);
        }
    }

    @Benchmark
    public void productionOneForcePerBatch(BenchmarkState state) {
        state.productionWal.appendLocal(
                1,
                state.batchRecords
        );
    }

    @Benchmark
    public void prototypeOneForcePerBatch(BenchmarkState state)
            throws IOException {
        for (int index = 0; index < state.batchSize; index++) {
            ByteBuffer frame = state.prototypeFrame.duplicate();
            while (frame.hasRemaining()) {
                state.prototypeChannel.write(frame);
            }
        }
        state.prototypeChannel.force(true);
    }

    @State(Scope.Thread)
    public static class BenchmarkState {
        @Param({"1", "8", "32", "128", "256"})
        private int batchSize;

        private Path directory;
        private SegmentedFileWriteAheadLog productionWal;
        private FileChannel prototypeChannel;
        private WalRecord record;
        private List<WalRecord> batchRecords;
        private ByteBuffer prototypeFrame;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("batch-force-jmh-");
            productionWal = new SegmentedFileWriteAheadLog(
                    directory.resolve("production-wal"),
                    512L * 1024 * 1024,
                    StorageLineage.create()
            );
            prototypeChannel = FileChannel.open(
                    directory.resolve("prototype.wal"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
            record = new WalRecord(
                    WalRecordType.PUBLISH,
                    "benchmark-message",
                    "x".repeat(1024),
                    null,
                    1,
                    Instant.parse("2026-09-03T00:00:00Z")
            );
            batchRecords = java.util.Collections.nCopies(
                    batchSize,
                    record
            );
            prototypeFrame = prototypeFrame(1024);
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            productionWal.close();
            prototypeChannel.close();
            BenchmarkFiles.deleteTree(directory);
        }
    }

    private static ByteBuffer prototypeFrame(int payloadBytes) {
        byte[] payload = new byte[payloadBytes];
        java.util.Arrays.fill(payload, (byte) 'x');
        CRC32C checksum = new CRC32C();
        checksum.update(payload, 0, payload.length);
        ByteBuffer frame = ByteBuffer.allocate(
                Integer.BYTES + payload.length + Integer.BYTES
        );
        frame.putInt(payload.length);
        frame.put(payload);
        frame.putInt((int) checksum.getValue());
        frame.flip();
        return frame.asReadOnlyBuffer();
    }
}
