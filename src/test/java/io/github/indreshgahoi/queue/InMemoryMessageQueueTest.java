package io.github.indreshgahoi.queue;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryMessageQueueTest {

    @Test
    void publishedMessageCanBeReceived() {
        MessageQueue queue = new InMemoryMessageQueue();

        String messageId = queue.publish("hello");

        Message message = queue.receive().orElseThrow();

        assertEquals(messageId, message.id());
        assertEquals("hello", message.payload());
    }

    @Test
    void messagesAreReceivedInFifoOrder() {

        MessageQueue queue = new InMemoryMessageQueue();

        queue.publish("A");
        queue.publish("B");
        queue.publish("C");

        assertEquals("A", queue.receive().orElseThrow().payload());
        assertEquals("B", queue.receive().orElseThrow().payload());
        assertEquals("C", queue.receive().orElseThrow().payload());

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

}