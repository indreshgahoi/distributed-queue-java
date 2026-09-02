package io.github.indreshgahoi.queue.benchmark;

import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeTopologyClient;
import io.github.indreshgahoi.queue.node.application.service.RuntimePartitionManager;
import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
import org.openjdk.jmh.infra.ThreadParams;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RuntimeAdmissionBenchmark {
    private static final Instant NOW =
            Instant.parse("2026-09-02T12:00:00Z");
    private static final Function<RuntimeQueue, RuntimeQueue> IDENTITY =
            Function.identity();

    @Benchmark
    public void readyQueueLookup(
            BenchmarkState state,
            Blackhole blackhole,
            ThreadParams thread
    ) {
        // Spread workers over independent handles when the topology permits.
        // The 1-versus-1000 comparison therefore exposes whether an admission
        // mechanism accidentally serializes unrelated runtime partitions.
        UUID queueId = state.queueIds.get(
                thread.getThreadIndex() % state.queueIds.size()
        );
        blackhole.consume(state.manager.withReadyQueue(
                queueId,
                IDENTITY
        ));
    }

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({"1", "1000"})
        private int activeQueueCount;

        private RuntimePartitionManager manager;
        private List<UUID> queueIds;

        @Setup
        public void setUp() {
            NodeRegistration registration = new NodeRegistration(
                    "benchmark-node",
                    1,
                    NOW.plus(Duration.ofDays(1))
            );
            List<PartitionPlacement> placements = new ArrayList<>();
            queueIds = new ArrayList<>();
            for (int index = 0; index < activeQueueCount; index++) {
                UUID queueId = UUID.randomUUID();
                queueIds.add(queueId);
                placements.add(new PartitionPlacement(
                        queueId,
                        UUID.randomUUID(),
                        0,
                        "benchmark-node",
                        1,
                        0
                ));
            }
            RuntimeTopologyClient topology = new FixedTopology(placements);
            manager = new RuntimePartitionManager(
                    "benchmark-node",
                    Clock.fixed(NOW, ZoneOffset.UTC),
                    () -> Optional.of(registration),
                    topology,
                    ignored -> new NoOpRuntimeQueue()
            );
            manager.runOnce();
        }

        @TearDown
        public void tearDown() {
            manager.close();
        }
    }

    private record FixedTopology(List<PartitionPlacement> placements)
            implements RuntimeTopologyClient {
        @Override
        public List<PartitionPlacement> activePlacements(
                NodeRegistration registration
        ) {
            return placements;
        }

        @Override
        public void publishStatus(
                RuntimePartitionIdentity identity,
                RuntimePartitionState state,
                String failureReason
        ) {
        }
    }

    private static final class NoOpRuntimeQueue implements RuntimeQueue {
        @Override
        public String publish(String payload) {
            return "unused";
        }

        @Override
        public Optional<MessageDelivery> receive() {
            return Optional.empty();
        }

        @Override
        public boolean ack(String receiptHandle) {
            return false;
        }

        @Override
        public boolean nack(String receiptHandle, Duration retryDelay) {
            return false;
        }

        @Override
        public void close() {
        }
    }
}
