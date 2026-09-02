package io.github.indreshgahoi.queue.benchmark;

import io.github.indreshgahoi.queue.Delivery;
import io.github.indreshgahoi.queue.LocalMessageQueue;
import io.github.indreshgahoi.queue.QueueConfiguration;
import io.github.indreshgahoi.queue.storage.StorageLineage;
import io.github.indreshgahoi.queue.storage.WalPosition;
import io.github.indreshgahoi.queue.storage.wal.SegmentedFileWriteAheadLog;
import io.github.indreshgahoi.queue.storage.wal.WalRecord;
import io.github.indreshgahoi.queue.storage.wal.WriteAheadLog;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LocalQueueBenchmark {
    private static final String PAYLOAD = "benchmark-message-payload";

    @Benchmark
    public void publishWithoutDurability(
            NonDurableState state,
            Blackhole blackhole
    ) {
        blackhole.consume(state.queue.publish(PAYLOAD));
    }

    @Benchmark
    public void publishWithForcedWal(
            DurablePublishState state,
            Blackhole blackhole
    ) {
        blackhole.consume(state.queue.publish(PAYLOAD));
    }

    @Benchmark
    public void durableReceiveAckCycle(
            DurableCycleState state,
            Blackhole blackhole
    ) {
        Delivery delivery = state.queue.receive().orElseThrow();
        if (!state.queue.ack(delivery.receiptHandle())) {
            throw new IllegalStateException("benchmark ACK failed");
        }
        blackhole.consume(state.queue.publish(PAYLOAD));
    }

    @State(Scope.Benchmark)
    public static class NonDurableState {
        private LocalMessageQueue queue;

        @Setup(Level.Iteration)
        public void setUp() {
            queue = new LocalMessageQueue(
                    Clock.systemUTC(),
                    new QueueConfiguration(),
                    new DiscardingWriteAheadLog()
            );
        }

        @TearDown(Level.Iteration)
        public void tearDown() {
            queue.close();
        }
    }

    @State(Scope.Benchmark)
    public static class DurablePublishState extends DurableState {
    }

    @State(Scope.Thread)
    public static class DurableCycleState extends DurableState {
        // A cycle is a compound receive/ACK/publish operation. Per-thread
        // state prevents one worker from consuming another worker's seed and
        // makes the concurrency result about independent durable queues.
        @Override
        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            super.setUp();
            queue.publish(PAYLOAD);
        }
    }

    public abstract static class DurableState {
        protected LocalMessageQueue queue;
        private Path directory;

        @Setup(Level.Iteration)
        public void setUp() throws IOException {
            directory = Files.createTempDirectory("queue-jmh-");
            queue = new LocalMessageQueue(
                    Clock.systemUTC(),
                    new QueueConfiguration(),
                    new SegmentedFileWriteAheadLog(
                            directory,
                            16 * 1024 * 1024,
                            StorageLineage.create()
                    )
            );
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws IOException {
            queue.close();
            try (var paths = Files.walk(directory)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException exception) {
                                throw new IllegalStateException(exception);
                            }
                        });
            }
        }
    }

    private static final class DiscardingWriteAheadLog
            implements WriteAheadLog {
        // This seam intentionally removes persistence cost while preserving
        // the production queue state machine exercised by publish().
        private final StorageLineage lineage = StorageLineage.create();

        @Override
        public void append(WalRecord record) {
        }

        @Override
        public List<WalRecord> readAll() {
            return List.of();
        }

        @Override
        public WalPosition currentDurablePosition() {
            return new WalPosition(0, 0);
        }

        @Override
        public List<WalRecord> readFrom(WalPosition position) {
            return List.of();
        }

        @Override
        public StorageLineage storageLineage() {
            return lineage;
        }

        @Override
        public void close() {
        }
    }
}
