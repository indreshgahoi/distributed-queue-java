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
        Message firstDelivery = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(29));

        int reQueued = queue.requeueExpireMessages();

        assertEquals(0, reQueued);
        assertTrue(queue.receive().isEmpty());
        assertEquals("A", firstDelivery.payload());
    }

    @Test
    void inFlightMessageIsRedeliveredAfterLeaseExpiry() {

        queue.publish("A");

        Message firstDelivery = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(31));

        int reQueued = queue.requeueExpireMessages();

        Message secondDelivery = queue.receive().orElseThrow();

        assertEquals(1, reQueued);
        assertEquals(firstDelivery.id(), secondDelivery.id());
        assertEquals(firstDelivery.payload(), secondDelivery.payload());

    }

    @Test
    void acknowledgedMessageIsNotReQueuedAfterLeaseExpiry() {

        queue.publish("A");

        Message firstDelivery = queue.receive().orElseThrow();

        boolean acked = queue.ack(firstDelivery.id());


        clock.advance(Duration.ofSeconds(31));

        int reQueued = queue.requeueExpireMessages();

        assertTrue(acked);
        assertEquals(0, reQueued);
        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void messageExpiresExactlyAtLeaseBoundary() {

        queue.publish("A");

        Message firstDelivery = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(30));

        int reQueued = queue.requeueExpireMessages();

        Message secondDelivery = queue.receive().orElseThrow();

        assertEquals(1, reQueued);
        assertEquals(firstDelivery.id(), secondDelivery.id());
        assertEquals(firstDelivery.payload(), secondDelivery.payload());

    }

    @Test
    void multipleExpiredMessagesAreReQueued(){

        queue.publish("A");
        queue.publish("B");
        queue.publish("C");

        Message firstDelivery = queue.receive().orElseThrow();
        Message secondDelivery = queue.receive().orElseThrow();
        Message thirdDelivery = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(31));

        int reQueued = queue.requeueExpireMessages();

        Set<Message> redeliveredMessages = Set.of(
                queue.receive().orElseThrow(),
                queue.receive().orElseThrow(),
                queue.receive().orElseThrow()
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

        Message a = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(20));

        queue.publish("B");

        Message b = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(11));

        int reQueued = queue.requeueExpireMessages();

        Message aRedelivered = queue.receive().orElseThrow();

        assertEquals(1, reQueued);
        assertEquals(aRedelivered.id(), a.id());
        assertEquals(aRedelivered.payload(), a.payload());
        assertNotEquals(aRedelivered.id(), b.id());
        assertNotEquals(aRedelivered.payload(), b.payload());
    }
    @Test
    void redeliveredMessageGetsANewLease() {

        queue.publish("A");

        Message first = queue.receive().orElseThrow();

        clock.advance(Duration.ofSeconds(31));
        queue.requeueExpireMessages();

        Message second = queue.receive().orElseThrow();

        assertEquals(first.id(), second.id());

        clock.advance(Duration.ofSeconds(29));

        assertEquals(0, queue.requeueExpireMessages());
        assertTrue(queue.receive().isEmpty());

        clock.advance(Duration.ofSeconds(1));

        assertEquals(1, queue.requeueExpireMessages());
        assertEquals(first.id(), queue.receive().orElseThrow().id());
    }

}
