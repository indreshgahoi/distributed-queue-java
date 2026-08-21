package io.github.indreshgahoi.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryMessageQueueConcurrencyTest {

    private MutableClock clock;
    private InMemoryMessageQueue queue;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(
                Instant.parse("2026-08-21T00:00:00Z")
        );

        QueueConfiguration config =
                new QueueConfiguration(
                        Duration.ofSeconds(30),
                        3
                );

        queue = new InMemoryMessageQueue(
                clock,
                config
        );
    }

    @Test
    void twoConsumersCannotReceiveSameMessage() throws Exception {
        queue.publish("A");

        ExecutorService executor =
                Executors.newFixedThreadPool(2);

        CountDownLatch start =
                new CountDownLatch(1);

        List<Future<Delivery>> futures =
                new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            futures.add(
                    executor.submit(() -> {
                        start.await();

                        return queue.receive()
                                .orElse(null);
                    })
            );
        }

        start.countDown();

        Delivery first =
                futures.get(0).get();

        Delivery second =
                futures.get(1).get();

        executor.shutdown();

        int received =
                (first != null ? 1 : 0)
                        + (second != null ? 1 : 0);

        assertEquals(1, received);
    }

    @Test
    void concurrentConsumersReceiveEachMessageOnlyOncePerAttempt()
            throws Exception {

        int messageCount = 10_000;
        int consumerCount = 20;

        for (int i = 0; i < messageCount; i++) {
            queue.publish("message-" + i);
        }

        ExecutorService executor =
                Executors.newFixedThreadPool(consumerCount);

        Set<String> receivedMessageIds =
                ConcurrentHashMap.newKeySet();

        AtomicInteger duplicateCount =
                new AtomicInteger();

        CountDownLatch start =
                new CountDownLatch(1);

        List<Future<?>> futures =
                new ArrayList<>();

        for (int i = 0; i < consumerCount; i++) {
            futures.add(
                    executor.submit(() -> {
                        start.await();

                        while (true) {
                            var delivery =
                                    queue.receive();

                            if (delivery.isEmpty()) {
                                break;
                            }

                            String messageId =
                                    delivery.get()
                                            .message()
                                            .id();

                            boolean firstSeen =
                                    receivedMessageIds.add(
                                            messageId
                                    );

                            if (!firstSeen) {
                                duplicateCount.incrementAndGet();
                            }
                        }

                        return null;
                    })
            );
        }

        start.countDown();

        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();

        assertEquals(
                messageCount,
                receivedMessageIds.size()
        );

        assertEquals(
                0,
                duplicateCount.get()
        );
    }

    @Test
    void ackAndNackCannotBothSucceed()
            throws Exception {

        queue.publish("A");

        Delivery delivery =
                queue.receive().orElseThrow();

        ExecutorService executor =
                Executors.newFixedThreadPool(2);

        CountDownLatch start =
                new CountDownLatch(1);

        Future<Boolean> ackResult =
                executor.submit(() -> {
                    start.await();

                    return queue.ack(
                            delivery.receiptHandle()
                    );
                });

        Future<Boolean> nackResult =
                executor.submit(() -> {
                    start.await();

                    return queue.nack(
                            delivery.receiptHandle(),
                            Duration.ofSeconds(5)
                    );
                });

        start.countDown();

        boolean ack =
                ackResult.get();

        boolean nack =
                nackResult.get();

        executor.shutdown();

        /*
         * Exactly one operation must win ownership
         * of the IN_FLIGHT -> terminal/next transition.
         */
        assertNotEquals(ack, nack);
    }

    @Test
    void concurrentAckCallsOnlyOneCanSucceed()
            throws Exception {

        queue.publish("A");

        Delivery delivery =
                queue.receive().orElseThrow();

        int threadCount = 20;

        ExecutorService executor =
                Executors.newFixedThreadPool(threadCount);

        CountDownLatch start =
                new CountDownLatch(1);

        AtomicInteger successfulAcks =
                new AtomicInteger();

        List<Future<?>> futures =
                new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(
                    executor.submit(() -> {
                        start.await();

                        if (queue.ack(
                                delivery.receiptHandle()
                        )) {
                            successfulAcks.incrementAndGet();
                        }

                        return null;
                    })
            );
        }

        start.countDown();

        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();

        assertEquals(
                1,
                successfulAcks.get()
        );
    }

    @Test
    void concurrentNackCallsOnlyOneCanSucceed()
            throws Exception {

        queue.publish("A");

        Delivery delivery =
                queue.receive().orElseThrow();

        int threadCount = 20;

        ExecutorService executor =
                Executors.newFixedThreadPool(threadCount);

        CountDownLatch start =
                new CountDownLatch(1);

        AtomicInteger successfulNacks =
                new AtomicInteger();

        List<Future<?>> futures =
                new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(
                    executor.submit(() -> {
                        start.await();

                        if (queue.nack(
                                delivery.receiptHandle(),
                                Duration.ofSeconds(5)
                        )) {
                            successfulNacks.incrementAndGet();
                        }

                        return null;
                    })
            );
        }

        start.countDown();

        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();

        assertEquals(
                1,
                successfulNacks.get()
        );

        clock.advance(Duration.ofSeconds(5));

        assertEquals(
                1,
                queue.makeDelayedMessagesReady()
        );

        assertTrue(queue.receive().isPresent());
        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void ackAndLeaseExpiryCannotCreateDuplicateMessage()
            throws Exception {

        queue.publish("A");

        Delivery delivery =
                queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(30));

        ExecutorService executor =
                Executors.newFixedThreadPool(2);

        CountDownLatch start =
                new CountDownLatch(1);

        Future<Boolean> ackResult =
                executor.submit(() -> {
                    start.await();

                    return queue.ack(
                            delivery.receiptHandle()
                    );
                });

        Future<Integer> expiryResult =
                executor.submit(() -> {
                    start.await();

                    return queue.requeueExpiredMessages();
                });

        start.countDown();

        boolean ackSucceeded =
                ackResult.get();

        int requeued =
                expiryResult.get();

        executor.shutdown();

        /*
         * Exactly one transition should occur.
         *
         * Either:
         *
         * IN_FLIGHT -> DONE
         *
         * or:
         *
         * IN_FLIGHT -> READY
         */
        assertTrue(
                (ackSucceeded && requeued == 0)
                        ||
                (!ackSucceeded && requeued == 1)
        );

        if (requeued == 1) {
            assertTrue(queue.receive().isPresent());
            assertTrue(queue.receive().isEmpty());
        } else {
            assertTrue(queue.receive().isEmpty());
        }
    }

    @Test
    void nackAndLeaseExpiryCannotBothTransitionSameDelivery()
            throws Exception {

        queue.publish("A");

        Delivery delivery =
                queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(30));

        ExecutorService executor =
                Executors.newFixedThreadPool(2);

        CountDownLatch start =
                new CountDownLatch(1);

        Future<Boolean> nackResult =
                executor.submit(() -> {
                    start.await();

                    return queue.nack(
                            delivery.receiptHandle(),
                            Duration.ofSeconds(5)
                    );
                });

        Future<Integer> expiryResult =
                executor.submit(() -> {
                    start.await();

                    return queue.requeueExpiredMessages();
                });

        start.countDown();

        boolean nackSucceeded =
                nackResult.get();

        int requeued =
                expiryResult.get();

        executor.shutdown();

        assertTrue(
                (nackSucceeded && requeued == 0)
                        ||
                (!nackSucceeded && requeued == 1)
        );
    }

    @Test
    void concurrentDelayedPromotionDoesNotDuplicateMessage()
            throws Exception {

        queue.publish("A");

        Delivery delivery =
                queue.receive().orElseThrow();

        queue.nack(
                delivery.receiptHandle(),
                Duration.ofSeconds(5)
        );

        clock.advance(Duration.ofSeconds(5));

        ExecutorService executor =
                Executors.newFixedThreadPool(10);

        CountDownLatch start =
                new CountDownLatch(1);

        AtomicInteger moved =
                new AtomicInteger();

        List<Future<?>> futures =
                new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            futures.add(
                    executor.submit(() -> {
                        start.await();

                        moved.addAndGet(
                                queue.makeDelayedMessagesReady()
                        );

                        return null;
                    })
            );
        }

        start.countDown();

        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();

        assertEquals(1, moved.get());

        assertTrue(queue.receive().isPresent());
        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void concurrentPublishDoesNotLoseMessages()
            throws Exception {

        int producerCount = 10;
        int messagesPerProducer = 1_000;

        ExecutorService executor =
                Executors.newFixedThreadPool(producerCount);

        CountDownLatch start =
                new CountDownLatch(1);

        List<Future<?>> futures =
                new ArrayList<>();

        for (int producer = 0;
             producer < producerCount;
             producer++) {

            int producerId = producer;

            futures.add(
                    executor.submit(() -> {
                        start.await();

                        for (int i = 0;
                             i < messagesPerProducer;
                             i++) {

                            queue.publish(
                                    "p-" + producerId
                                            + "-m-" + i
                            );
                        }

                        return null;
                    })
            );
        }

        start.countDown();

        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();

        int received = 0;

        while (queue.receive().isPresent()) {
            received++;
        }

        assertEquals(
                producerCount * messagesPerProducer,
                received
        );
    }
}