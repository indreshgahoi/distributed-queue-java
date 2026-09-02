package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimePartitionHandleTest {
    @Test
    void closingRejectsNewAdmissionsAndWaitsForAdmittedOperation()
            throws Exception {
        CountingRuntimeQueue queue = new CountingRuntimeQueue();
        RuntimePartitionHandle handle = new RuntimePartitionHandle(
                identity(),
                queue
        );
        RuntimePartitionHandle.Admission admission =
                handle.tryAcquire().orElseThrow();
        handle.beginClosing();

        try (ExecutorService executor =
                     Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> close = executor.submit(handle::close);

            assertTrue(handle.tryAcquire().isEmpty());
            assertFalse(close.isDone());
            assertEquals(0, queue.closes.get());

            admission.close();
            close.get(5, TimeUnit.SECONDS);
            assertEquals(1, queue.closes.get());
        }
    }

    @Test
    void concurrentCloseClosesRuntimeQueueExactlyOnce() throws Exception {
        CountingRuntimeQueue queue = new CountingRuntimeQueue();
        RuntimePartitionHandle handle = new RuntimePartitionHandle(
                identity(),
                queue
        );

        try (ExecutorService executor =
                     Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> first = executor.submit(handle::close);
            Future<?> second = executor.submit(handle::close);

            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        }

        assertEquals(1, queue.closes.get());
        assertTrue(handle.tryAcquire().isEmpty());
    }

    @Test
    void exceptionalOperationPathReleasesPermitBeforeClose() {
        CountingRuntimeQueue queue = new CountingRuntimeQueue();
        RuntimePartitionHandle handle = new RuntimePartitionHandle(
                identity(),
                queue
        );

        assertThrows(IllegalStateException.class, () -> {
            try (RuntimePartitionHandle.Admission ignored =
                         handle.tryAcquire().orElseThrow()) {
                throw new IllegalStateException("operation failed");
            }
        });

        handle.close();
        assertEquals(1, queue.closes.get());
    }

    private RuntimePartitionIdentity identity() {
        return new RuntimePartitionIdentity(
                UUID.randomUUID(),
                UUID.randomUUID(),
                0,
                "node-a",
                1,
                1
        );
    }

    private static final class CountingRuntimeQueue implements RuntimeQueue {
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public String publish(String payload) {
            return "message-id";
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
            closes.incrementAndGet();
        }
    }
}
