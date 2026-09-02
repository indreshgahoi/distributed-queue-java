package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.out.NodeRegistrationProvider;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueueFactory;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeTopologyClient;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionState;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimePartitionManagerTest {
    private static final Instant NOW =
            Instant.parse("2026-09-02T12:00:00Z");
    private static final UUID QUEUE_ID = UUID.randomUUID();
    private static final UUID GENERATION_ID = UUID.randomUUID();
    private static final PartitionPlacement PLACEMENT =
            new PartitionPlacement(
                    QUEUE_ID,
                    GENERATION_ID,
                    0,
                    "node-a",
                    1,
                    0
            );

    @Test
    void assignedPartitionIsRecoveredAndPublishedReady() {
        MutableRegistrations registrations = registered(1);
        FakeTopology topology = new FakeTopology(List.of(PLACEMENT));
        FakeQueueFactory queues = new FakeQueueFactory();
        RuntimePartitionManager manager = manager(
                registrations,
                topology,
                queues
        );

        manager.runOnce();

        assertEquals(1, queues.opens.get());
        assertEquals(1, manager.partitions().size());
        assertEquals(
                RuntimePartitionState.READY,
                manager.partitions().getFirst().state()
        );
        assertEquals(List.of(RuntimePartitionState.READY), topology.states);
    }

    @Test
    void repeatedReconciliationDoesNotOpenDuplicateRuntime() {
        FakeQueueFactory queues = new FakeQueueFactory();
        RuntimePartitionManager manager = manager(
                registered(1),
                new FakeTopology(List.of(PLACEMENT)),
                queues
        );

        manager.runOnce();
        manager.runOnce();

        assertEquals(1, queues.opens.get());
    }

    @Test
    void registrationLossClosesEveryActiveRuntime() {
        MutableRegistrations registrations = registered(1);
        FakeQueueFactory queues = new FakeQueueFactory();
        RuntimePartitionManager manager = manager(
                registrations,
                new FakeTopology(List.of(PLACEMENT)),
                queues
        );
        manager.runOnce();

        registrations.current = null;
        manager.runOnce();

        assertEquals(1, queues.closes.get());
        assertTrue(manager.partitions().isEmpty());
    }

    @Test
    void placementEpochChangeClosesOldRuntimeAndOpensNewAuthority() {
        MutableRegistrations registrations = registered(1);
        FakeTopology topology = new FakeTopology(List.of(PLACEMENT));
        FakeQueueFactory queues = new FakeQueueFactory();
        RuntimePartitionManager manager = manager(
                registrations,
                topology,
                queues
        );
        manager.runOnce();

        topology.placements = List.of(new PartitionPlacement(
                QUEUE_ID,
                GENERATION_ID,
                0,
                "node-a",
                2,
                1
        ));
        manager.runOnce();

        assertEquals(2, queues.opens.get());
        assertEquals(1, queues.closes.get());
        assertEquals(
                2,
                manager.partitions().getFirst()
                        .identity().placementEpoch()
        );
    }

    @Test
    void registrationChangingDuringRecoveryDiscardsRecoveredQueue() {
        MutableRegistrations registrations = registered(1);
        FakeTopology topology = new FakeTopology(List.of(PLACEMENT));
        FakeQueueFactory queues = new FakeQueueFactory();
        queues.afterOpen = () -> registrations.current = registration(2);
        RuntimePartitionManager manager = manager(
                registrations,
                topology,
                queues
        );

        manager.runOnce();

        assertEquals(1, queues.closes.get());
        assertTrue(manager.partitions().isEmpty());
        assertTrue(topology.states.isEmpty());
    }

    @Test
    void recoveryFailureIsPublishedAndDoesNotBlockAnotherPartition() {
        PartitionPlacement second = new PartitionPlacement(
                UUID.randomUUID(),
                UUID.randomUUID(),
                0,
                "node-a",
                1,
                0
        );
        FakeTopology topology = new FakeTopology(
                List.of(PLACEMENT, second)
        );
        FakeQueueFactory queues = new FakeQueueFactory();
        queues.failingQueueId = QUEUE_ID;
        RuntimePartitionManager manager = manager(
                registered(1),
                topology,
                queues
        );

        manager.runOnce();

        assertEquals(2, queues.opens.get());
        assertEquals(2, manager.partitions().size());
        assertTrue(topology.states.contains(RuntimePartitionState.FAILED));
        assertTrue(topology.states.contains(RuntimePartitionState.READY));
    }

    @Test
    void rejectedReadyPublicationClosesRecoveredQueue() {
        FakeTopology topology = new FakeTopology(List.of(PLACEMENT));
        topology.rejectReady = true;
        FakeQueueFactory queues = new FakeQueueFactory();
        RuntimePartitionManager manager = manager(
                registered(1),
                topology,
                queues
        );

        manager.runOnce();

        assertEquals(1, queues.closes.get());
        assertEquals(
                RuntimePartitionState.FAILED,
                manager.partitions().getFirst().state()
        );
    }

    private RuntimePartitionManager manager(
            MutableRegistrations registrations,
            FakeTopology topology,
            FakeQueueFactory queues
    ) {
        return new RuntimePartitionManager(
                "node-a",
                Clock.fixed(NOW, ZoneOffset.UTC),
                registrations,
                topology,
                queues
        );
    }

    private MutableRegistrations registered(long epoch) {
        return new MutableRegistrations(registration(epoch));
    }

    private NodeRegistration registration(long epoch) {
        return new NodeRegistration(
                "node-a",
                epoch,
                NOW.plusSeconds(30)
        );
    }

    private static final class MutableRegistrations
            implements NodeRegistrationProvider {
        private NodeRegistration current;

        private MutableRegistrations(NodeRegistration current) {
            this.current = current;
        }

        @Override
        public Optional<NodeRegistration> currentRegistration() {
            return Optional.ofNullable(current);
        }
    }

    private static final class FakeTopology
            implements RuntimeTopologyClient {
        private List<PartitionPlacement> placements;
        private final List<RuntimePartitionState> states = new ArrayList<>();
        private boolean rejectReady;

        private FakeTopology(List<PartitionPlacement> placements) {
            this.placements = placements;
        }

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
            if (rejectReady && state == RuntimePartitionState.READY) {
                throw new IllegalStateException("stale authority");
            }
            states.add(state);
        }
    }

    private static final class FakeQueueFactory
            implements RuntimeQueueFactory {
        private final AtomicInteger opens = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private Runnable afterOpen = () -> { };
        private UUID failingQueueId;

        @Override
        public RuntimeQueue open(PartitionPlacement placement) {
            opens.incrementAndGet();
            if (placement.queueId().equals(failingQueueId)) {
                throw new IllegalStateException("recovery failed");
            }
            afterOpen.run();
            return new RuntimeQueue() {
                @Override
                public String publish(String payload) {
                    return "message-id";
                }

                @Override
                public Optional<io.github.indreshgahoi.queue.node.domain.model.MessageDelivery>
                receive() {
                    return Optional.empty();
                }

                @Override
                public boolean ack(String receiptHandle) {
                    return true;
                }

                @Override
                public boolean nack(
                        String receiptHandle,
                        Duration retryDelay
                ) {
                    return true;
                }

                @Override
                public void close() {
                    closes.incrementAndGet();
                }
            };
        }
    }
}
