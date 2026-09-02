package io.github.indreshgahoi.queue.node.adapter.out.storage;

import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.config.QueueNodeProperties;
import io.github.indreshgahoi.queue.node.domain.model.MessageDelivery;
import io.github.indreshgahoi.queue.node.domain.model.PartitionPlacement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalRuntimeQueueFactoryTest {
    @TempDir
    private Path storageRoot;

    @Test
    void adapterDelegatesDurableMessageLifecycle() {
        PartitionPlacement placement = placement();
        LocalRuntimeQueueFactory factory = factory();
        String messageId;

        try (RuntimeQueue queue = factory.open(placement)) {
            messageId = queue.publish("payload");
        }

        try (RuntimeQueue recovered = factory.open(placement)) {
            MessageDelivery delivery = recovered.receive().orElseThrow();
            assertEquals(messageId, delivery.messageId());
            assertEquals("payload", delivery.payload());
            assertEquals(1, delivery.attempt());
            assertTrue(recovered.ack(delivery.receiptHandle()));
            assertFalse(recovered.ack(delivery.receiptHandle()));
        }
    }

    private LocalRuntimeQueueFactory factory() {
        return new LocalRuntimeQueueFactory(new QueueNodeProperties(
                "node-a",
                URI.create("http://node-a:8081"),
                URI.create("http://localhost:8080"),
                storageRoot,
                Duration.ofSeconds(30),
                Duration.ofSeconds(30),
                1024
        ));
    }

    private PartitionPlacement placement() {
        return new PartitionPlacement(
                UUID.randomUUID(),
                UUID.randomUUID(),
                0,
                "node-a",
                1,
                0
        );
    }
}
