package io.github.indreshgahoi.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;


public class InMemoryMessageQueueRetryTest {

    private InMemoryMessageQueue queue;
    private MutableClock clock;


    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-08-19T10:00:00Z"));
        queue = new InMemoryMessageQueue(clock, new QueueConfiguration());
    }

    @Test
    void firstDeliveryHasAttemptOne(){
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        assertEquals(1, delivery.attempt());
    }

    @Test
    void redeliveryIncrementsAttemptNumber() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(30));
        queue.requeueExpiredMessages();

        Delivery second = queue.receive().orElseThrow();

        assertEquals(1, first.attempt());
        assertEquals(2, second.attempt());
        assertEquals(first.message().id(), second.message().id());
        assertNotEquals(first.receiptHandle(), second.receiptHandle());
    }

    @Test
    void messageMovesToDeadLetterAfterFinalAttemptExpires() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();
        assertEquals(1, first.attempt());

        clock.advance(Duration.ofSeconds(30));
        queue.requeueExpiredMessages();

        Delivery second = queue.receive().orElseThrow();
        assertEquals(2, second.attempt());

        clock.advance(Duration.ofSeconds(30));
        queue.requeueExpiredMessages();

        Delivery third = queue.receive().orElseThrow();
        assertEquals(3, third.attempt());

        clock.advance(Duration.ofSeconds(30));

        int requeued = queue.requeueExpiredMessages();

        assertEquals(0, requeued);
        assertTrue(queue.receive().isEmpty());
        assertEquals(1, queue.deadLetterCount());
    }

    @Test
    void successfulAckOnFinalAttemptDoesNotDeadLetterMessage() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(30));
        queue.requeueExpiredMessages();

        Delivery second = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(30));
        queue.requeueExpiredMessages();

        Delivery third = queue.receive().orElseThrow();

        assertTrue(queue.ack(third.receiptHandle()));

        clock.advance(Duration.ofMinutes(1));
        queue.requeueExpiredMessages();

        assertEquals(0, queue.deadLetterCount());
        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void deadLetterMessageIsNotRedelivered() {
        // exhaust all attempts

        assertTrue(queue.receive().isEmpty());

        clock.advance(Duration.ofMinutes(10));
        queue.requeueExpiredMessages();

        assertTrue(queue.receive().isEmpty());
    }
}
