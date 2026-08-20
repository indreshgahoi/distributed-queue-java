package io.github.indreshgahoi.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;


class InMemoryMessageQueueLeaseTest {

    private InMemoryMessageQueue queue;
    private MutableClock clock;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-08-19T10:00:00Z"));
        queue = new InMemoryMessageQueue(clock);
    }

    @Test
    void inFlightMessageIsNotRedeliveredBeforeLeaseExpiry() {

        queue.publish("A");
        Message firstDelivery = queue.receive().orElseThrow().message();

        clock.advance(Duration.ofSeconds(29));

        int reQueued = queue.requeueExpiredMessages();

        assertEquals(0, reQueued);
        assertTrue(queue.receive().isEmpty());
        assertEquals("A", firstDelivery.payload());
    }

    @Test
    void inFlightMessageIsRedeliveredAfterLeaseExpiry() {

        queue.publish("A");

        Message firstDelivery = queue.receive().orElseThrow().message();

        clock.advance(Duration.ofSeconds(31));

        int reQueued = queue.requeueExpiredMessages();

        Message secondDelivery = queue.receive().orElseThrow().message();

        assertEquals(1, reQueued);
        assertEquals(firstDelivery.id(), secondDelivery.id());
        assertEquals(firstDelivery.payload(), secondDelivery.payload());

    }

    @Test
    void acknowledgedMessageIsNotReQueuedAfterLeaseExpiry() {

        queue.publish("A");

        Delivery firstDelivery = queue.receive().orElseThrow();

        boolean acked = queue.ack(firstDelivery.receiptHandle());


        clock.advance(Duration.ofSeconds(31));

        int reQueued = queue.requeueExpiredMessages();

        assertTrue(acked);
        assertEquals(0, reQueued);
        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void messageExpiresExactlyAtLeaseBoundary() {

        queue.publish("A");

        Message firstDelivery = queue.receive().orElseThrow().message();

        clock.advance(Duration.ofSeconds(30));

        int reQueued = queue.requeueExpiredMessages();

        Message secondDelivery = queue.receive().orElseThrow().message();

        assertEquals(1, reQueued);
        assertEquals(firstDelivery.id(), secondDelivery.id());
        assertEquals(firstDelivery.payload(), secondDelivery.payload());

    }

    @Test
    void multipleExpiredMessagesAreReQueued(){

        queue.publish("A");
        queue.publish("B");
        queue.publish("C");

        Message firstDelivery = queue.receive().orElseThrow().message();
        Message secondDelivery = queue.receive().orElseThrow().message();
        Message thirdDelivery = queue.receive().orElseThrow().message();

        clock.advance(Duration.ofSeconds(31));

        int reQueued = queue.requeueExpiredMessages();

        Set<Message> redeliveredMessages = Set.of(
                queue.receive().orElseThrow().message(),
                queue.receive().orElseThrow().message(),
                queue.receive().orElseThrow().message()
        );

        assertEquals(3, reQueued);
        assertEquals(
                Set.of(firstDelivery, secondDelivery, thirdDelivery),
                redeliveredMessages
        );

    }

    @Test
    void onlyExpiredMessagesAreReQueued() {

        queue.publish("A");

        Message a = queue.receive().orElseThrow().message();

        clock.advance(Duration.ofSeconds(20));

        queue.publish("B");

        Message b = queue.receive().orElseThrow().message();

        clock.advance(Duration.ofSeconds(11));

        int reQueued = queue.requeueExpiredMessages();

        Message aRedelivered = queue.receive().orElseThrow().message();

        assertEquals(1, reQueued);
        assertEquals(aRedelivered.id(), a.id());
        assertEquals(aRedelivered.payload(), a.payload());
        assertNotEquals(aRedelivered.id(), b.id());
        assertNotEquals(aRedelivered.payload(), b.payload());
    }
    @Test
    void redeliveredMessageGetsANewLease() {

        queue.publish("A");

        Message first = queue.receive().orElseThrow().message();

        clock.advance(Duration.ofSeconds(31));
        queue.requeueExpiredMessages();

        Message second = queue.receive().orElseThrow().message();

        assertEquals(first.id(), second.id());

        clock.advance(Duration.ofSeconds(29));

        assertEquals(0, queue.requeueExpiredMessages());
        assertTrue(queue.receive().isEmpty());

        clock.advance(Duration.ofSeconds(1));

        assertEquals(1, queue.requeueExpiredMessages());
        assertEquals(first.id(), queue.receive().orElseThrow().message().id());
    }

    @Test
    void redeliveryUsesSameMessageIdButNewReceiptHandle() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(30));
        queue.requeueExpiredMessages();

        Delivery second = queue.receive().orElseThrow();

        assertEquals(
                first.message().id(),
                second.message().id()
        );

        assertNotEquals(
                first.receiptHandle(),
                second.receiptHandle()
        );
    }

    @Test
    void staleReceiptHandleCannotAcknowledgeNewDelivery() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(30));
        queue.requeueExpiredMessages();

        Delivery second = queue.receive().orElseThrow();

        assertFalse(queue.ack(first.receiptHandle()));

        // The second delivery must still be active.
        assertTrue(queue.ack(second.receiptHandle()));
    }

    @Test
    void expiredReceiptHandleBecomesInvalid() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(30));
        queue.requeueExpiredMessages();

        assertFalse(queue.ack(delivery.receiptHandle()));
    }

    @Test
    void currentReceiptHandleCanAcknowledgeMessage() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        assertTrue(queue.ack(delivery.receiptHandle()));
        assertFalse(queue.ack(delivery.receiptHandle()));
    }

}
