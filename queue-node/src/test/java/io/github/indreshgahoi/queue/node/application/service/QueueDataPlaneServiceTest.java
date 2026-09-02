package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.out.NodeRegistrationProvider;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueueFactory;
import io.github.indreshgahoi.queue.node.application.port.out.RuntimeTopologyClient;
import io.github.indreshgahoi.queue.node.domain.exception.RuntimePartitionUnavailableException;
import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;
import io.github.indreshgahoi.queue.node.domain.model.NodeRegistration;
import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionState;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueueDataPlaneServiceTest {
    private static final Instant NOW =
            Instant.parse("2026-09-02T12:00:00Z");
    private static final UUID QUEUE_ID = UUID.randomUUID();

    @Test
    void delegatesMessageOperationsToReadyRuntime() {
        TestRuntimeQueue queue = new TestRuntimeQueue();
        TestContext context = readyContext(queue);
        QueueDataPlaneService service = new QueueDataPlaneService(
                context.manager()
        );

        assertEquals("message-1", service.publish(QUEUE_ID, "payload"));
        assertEquals(
                "receipt-1",
                service.receive(QUEUE_ID).orElseThrow().receiptHandle()
        );
        assertTrue(service.ack(QUEUE_ID, "receipt-1"));
        assertFalse(service.ack(QUEUE_ID, "stale-receipt"));
        assertTrue(service.nack(
                QUEUE_ID,
                "receipt-1",
                Duration.ofSeconds(5)
        ));
    }

    @Test
    void rejectsOperationWhenQueueIsNotReadyOnNode() {
        RuntimePartitionManager manager = manager(
                () -> Optional.of(registration()),
                new TestTopology(List.of()),
                placement -> new TestRuntimeQueue()
        );
        QueueDataPlaneService service = new QueueDataPlaneService(manager);

        assertThrows(
                RuntimePartitionUnavailableException.class,
                () -> service.publish(QUEUE_ID, "payload")
        );
    }

    @Test
    void requestAdmissionRejectsSupersededRegistrationBeforeNextPoll() {
        TestRuntimeQueue queue = new TestRuntimeQueue();
        MutableRegistration registration = new MutableRegistration();
        RuntimePartitionManager manager = manager(
                registration,
                new TestTopology(List.of(placement())),
                ignored -> queue
        );
        manager.runOnce();
        registration.current = new NodeRegistration(
                "node-a",
                2,
                NOW.plusSeconds(30)
        );

        assertThrows(
                RuntimePartitionUnavailableException.class,
                () -> new QueueDataPlaneService(manager).publish(
                        QUEUE_ID,
                        "payload"
                )
        );
        assertTrue(manager.partitions().isEmpty());
    }

    @Test
    void deactivationWaitsForInProgressOperationAndRejectsNewWork()
            throws Exception {
        BlockingRuntimeQueue queue = new BlockingRuntimeQueue();
        MutableRegistration registration = new MutableRegistration();
        RuntimePartitionManager manager = manager(
                registration,
                new TestTopology(List.of(placement())),
                ignored -> queue
        );
        manager.runOnce();
        QueueDataPlaneService service = new QueueDataPlaneService(manager);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<String> publish = executor.submit(
                    () -> service.publish(QUEUE_ID, "payload")
            );
            assertTrue(queue.entered.await(5, TimeUnit.SECONDS));

            registration.current = null;
            Future<?> reconcile = executor.submit(manager::runOnce);
            assertFalse(queue.closed.await(100, TimeUnit.MILLISECONDS));

            queue.release.countDown();
            assertEquals("message-1", publish.get(5, TimeUnit.SECONDS));
            reconcile.get(5, TimeUnit.SECONDS);
            assertTrue(queue.closed.await(5, TimeUnit.SECONDS));
            assertThrows(
                    RuntimePartitionUnavailableException.class,
                    () -> service.publish(QUEUE_ID, "another")
            );
        }
    }

    @Test
    void deactivatingBlockedQueueDoesNotBlockOperationOnAnotherQueue()
            throws Exception {
        UUID secondQueueId = UUID.randomUUID();
        BlockingRuntimeQueue blockedQueue = new BlockingRuntimeQueue();
        TestRuntimeQueue independentQueue = new TestRuntimeQueue();
        PartitionPlacement secondPlacement = placement(secondQueueId);
        TestTopology topology = new TestTopology(List.of(
                placement(),
                secondPlacement
        ));
        RuntimePartitionManager manager = manager(
                new MutableRegistration(),
                topology,
                placement -> placement.queueId().equals(QUEUE_ID)
                        ? blockedQueue
                        : independentQueue
        );
        manager.runOnce();
        QueueDataPlaneService service = new QueueDataPlaneService(manager);

        try (ExecutorService executor =
                     Executors.newVirtualThreadPerTaskExecutor()) {
            Future<String> blockedPublish = executor.submit(
                    () -> service.publish(QUEUE_ID, "slow")
            );
            assertTrue(blockedQueue.entered.await(5, TimeUnit.SECONDS));

            topology.placements = List.of(secondPlacement);
            topology.blockNextQuery = true;
            Future<?> reconcile = executor.submit(manager::runOnce);
            assertTrue(topology.queryEntered.await(5, TimeUnit.SECONDS));

            Future<String> independentPublish = executor.submit(
                    () -> service.publish(secondQueueId, "independent")
            );
            assertEquals(
                    "message-1",
                    independentPublish.get(5, TimeUnit.SECONDS)
            );

            topology.releaseQuery.countDown();
            blockedQueue.release.countDown();
            assertEquals(
                    "message-1",
                    blockedPublish.get(5, TimeUnit.SECONDS)
            );
            reconcile.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void negativeNackDelayIsRejectedBeforeRuntimeMutation() {
        TestRuntimeQueue queue = new TestRuntimeQueue();
        QueueDataPlaneService service = new QueueDataPlaneService(
                readyContext(queue).manager()
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> service.nack(
                        QUEUE_ID,
                        "receipt-1",
                        Duration.ofSeconds(-1)
                )
        );
        assertEquals(0, queue.nacks);
    }

    private TestContext readyContext(TestRuntimeQueue queue) {
        RuntimePartitionManager manager = manager(
                () -> Optional.of(registration()),
                new TestTopology(List.of(placement())),
                ignored -> queue
        );
        manager.runOnce();
        return new TestContext(manager);
    }

    private RuntimePartitionManager manager(
            NodeRegistrationProvider registration,
            RuntimeTopologyClient topology,
            RuntimeQueueFactory factory
    ) {
        return new RuntimePartitionManager(
                "node-a",
                Clock.fixed(NOW, ZoneOffset.UTC),
                registration,
                topology,
                factory
        );
    }

    private NodeRegistration registration() {
        return new NodeRegistration("node-a", 1, NOW.plusSeconds(30));
    }

    private PartitionPlacement placement() {
        return placement(QUEUE_ID);
    }

    private PartitionPlacement placement(UUID queueId) {
        return new PartitionPlacement(
                queueId,
                UUID.randomUUID(),
                0,
                "node-a",
                1,
                0
        );
    }

    private record TestContext(RuntimePartitionManager manager) {
    }

    private static final class MutableRegistration
            implements NodeRegistrationProvider {
        private NodeRegistration current = registrationStatic();

        @Override
        public Optional<NodeRegistration> currentRegistration() {
            return Optional.ofNullable(current);
        }
    }

    private static final class TestTopology
            implements RuntimeTopologyClient {
        private volatile List<PartitionPlacement> placements;
        private volatile boolean blockNextQuery;
        private final CountDownLatch queryEntered = new CountDownLatch(1);
        private final CountDownLatch releaseQuery = new CountDownLatch(1);

        private TestTopology(List<PartitionPlacement> placements) {
            this.placements = placements;
        }

        @Override
        public List<PartitionPlacement> activePlacements(
                NodeRegistration registration
        ) {
            if (blockNextQuery) {
                queryEntered.countDown();
                try {
                    if (!releaseQuery.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException(
                                "topology query release timed out"
                        );
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(exception);
                }
            }
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

    private static class TestRuntimeQueue implements RuntimeQueue {
        private int nacks;

        @Override
        public String publish(String payload) {
            return "message-1";
        }

        @Override
        public Optional<MessageDelivery> receive() {
            return Optional.of(new MessageDelivery(
                    "message-1",
                    "payload",
                    "receipt-1",
                    1
            ));
        }

        @Override
        public boolean ack(String receiptHandle) {
            return "receipt-1".equals(receiptHandle);
        }

        @Override
        public boolean nack(String receiptHandle, Duration retryDelay) {
            nacks++;
            return "receipt-1".equals(receiptHandle);
        }

        @Override
        public void close() {
        }
    }

    private static final class BlockingRuntimeQueue
            extends TestRuntimeQueue {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);

        @Override
        public String publish(String payload) {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("test release timed out");
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(exception);
            }
            return super.publish(payload);
        }

        @Override
        public void close() {
            closed.countDown();
        }
    }

    private static NodeRegistration registrationStatic() {
        return new NodeRegistration("node-a", 1, NOW.plusSeconds(30));
    }
}
