package io.github.indreshgahoi.queue;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryMessageQueueTest {

    @Test
    void publishedMessageCanBeReceived() {
        MessageQueue queue = new InMemoryMessageQueue();

        String messageId = queue.publish("hello");

        Message message = queue.receive().orElseThrow().message();

        assertEquals(messageId, message.id());
        assertEquals("hello", message.payload());
    }

    @Test
    void messagesAreReceivedInFifoOrder() {

        MessageQueue queue = new InMemoryMessageQueue();

        queue.publish("A");
        queue.publish("B");
        queue.publish("C");

        assertEquals("A", queue.receive().orElseThrow().message().payload());
        assertEquals("B", queue.receive().orElseThrow().message().payload());
        assertEquals("C", queue.receive().orElseThrow().message().payload());

    }

    @Test
    void receiveFromEmptyQueueReturnsEmpty(){

        MessageQueue queue = new InMemoryMessageQueue();

        assertTrue(queue.receive().isEmpty());

    }

    @Test
    void receivedMessageIsRemovedFromQueue() {
        MessageQueue queue = new InMemoryMessageQueue();

        queue.publish("A");

        queue.receive().orElseThrow();

        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void receivedMessageMovesToInFlight() {
        MessageQueue queue = new InMemoryMessageQueue();

        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        assertTrue(queue.receive().isEmpty());

        assertTrue(queue.ack(first.receiptHandle()));
    }

    @Test
    void ackRemovesInFlightMessage() {
        MessageQueue queue = new InMemoryMessageQueue();

        queue.publish("A");

        String receiptHandle = queue.receive().orElseThrow().receiptHandle();

        assertTrue(queue.ack(receiptHandle));
        assertFalse(queue.ack(receiptHandle));
    }

    @Test
    void ackOfUnknownMessageReturnsFalse() {
        MessageQueue queue = new InMemoryMessageQueue();

        assertFalse(queue.ack("unknown"));
    }

    @Test
    void receiveReturnsReceiptHandle() {
        MessageQueue queue = new InMemoryMessageQueue();
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        assertNotNull(delivery.receiptHandle());
        assertFalse(delivery.receiptHandle().isBlank());
    }

}