package io.github.indreshgahoi.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class LocalMessageQueueNackTest {

    private static final Duration VISIBILITY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration RETRY_DELAY = Duration.ofSeconds(10);
    private static final int MAX_DELIVERY_ATTEMPTS = 3;

    private MutableClock clock;
    private LocalMessageQueue queue;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(
                Instant.parse("2026-08-21T00:00:00Z")
        );

        queue = new LocalMessageQueue(clock, new QueueConfiguration(
                VISIBILITY_TIMEOUT,
                MAX_DELIVERY_ATTEMPTS
        ));
    }

    @Test
    void nackMovesMessageOutOfInFlight() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        assertTrue(
                queue.nack(
                        delivery.receiptHandle(),
                        RETRY_DELAY
                )
        );

        // Receipt handle became invalid after successful NACK.
        assertFalse(queue.ack(delivery.receiptHandle()));
        assertFalse(
                queue.nack(
                        delivery.receiptHandle(),
                        RETRY_DELAY
                )
        );
    }

    @Test
    void nackedMessageIsNotImmediatelyAvailable() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        assertTrue(
                queue.nack(
                        delivery.receiptHandle(),
                        RETRY_DELAY
                )
        );

        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void delayedMessageIsNotReadyBeforeRetryTime() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        queue.nack(
                delivery.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(Duration.ofSeconds(9));

        int moved = queue.makeDelayedMessagesReady();

        assertEquals(0, moved);
        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void delayedMessageBecomesReadyExactlyAtRetryBoundary() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        queue.nack(
                first.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(Duration.ofSeconds(10));

        int moved = queue.makeDelayedMessagesReady();

        assertEquals(1, moved);

        Delivery second = queue.receive().orElseThrow();

        assertEquals(
                first.message().id(),
                second.message().id()
        );
    }

    @Test
    void delayedMessageBecomesReadyAfterRetryTime() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        queue.nack(
                first.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(Duration.ofSeconds(11));

        assertEquals(
                1,
                queue.makeDelayedMessagesReady()
        );

        Delivery second = queue.receive().orElseThrow();

        assertEquals(
                first.message().id(),
                second.message().id()
        );
    }

    @Test
    void nackPreservesMessageIdentity() {
        String messageId = queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        queue.nack(
                first.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(RETRY_DELAY);
        queue.makeDelayedMessagesReady();

        Delivery second = queue.receive().orElseThrow();

        assertEquals(messageId, first.message().id());
        assertEquals(messageId, second.message().id());
    }

    @Test
    void redeliveryAfterNackGetsNewReceiptHandle() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        queue.nack(
                first.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(RETRY_DELAY);
        queue.makeDelayedMessagesReady();

        Delivery second = queue.receive().orElseThrow();

        assertNotEquals(
                first.receiptHandle(),
                second.receiptHandle()
        );
    }

    @Test
    void redeliveryAfterNackIncrementsAttempt() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        assertEquals(1, first.attempt());

        queue.nack(
                first.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(RETRY_DELAY);
        queue.makeDelayedMessagesReady();

        Delivery second = queue.receive().orElseThrow();

        assertEquals(2, second.attempt());
    }

    @Test
    void multipleNacksIncrementAttemptForEveryRedelivery() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();
        assertEquals(1, first.attempt());

        queue.nack(
                first.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(RETRY_DELAY);
        queue.makeDelayedMessagesReady();

        Delivery second = queue.receive().orElseThrow();
        assertEquals(2, second.attempt());

        queue.nack(
                second.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(RETRY_DELAY);
        queue.makeDelayedMessagesReady();

        Delivery third = queue.receive().orElseThrow();

        assertEquals(3, third.attempt());
    }

    @Test
    void nackOnFinalAttemptMovesMessageDirectlyToDeadLetter() {
        queue.publish("A");

        // attempt 1
        Delivery first = queue.receive().orElseThrow();

        assertTrue(
                queue.nack(
                        first.receiptHandle(),
                        RETRY_DELAY
                )
        );

        clock.advance(RETRY_DELAY);
        queue.makeDelayedMessagesReady();

        // attempt 2
        Delivery second = queue.receive().orElseThrow();

        assertTrue(
                queue.nack(
                        second.receiptHandle(),
                        RETRY_DELAY
                )
        );

        clock.advance(RETRY_DELAY);
        queue.makeDelayedMessagesReady();

        // attempt 3 - final permitted delivery
        Delivery third = queue.receive().orElseThrow();

        assertEquals(3, third.attempt());

        assertTrue(
                queue.nack(
                        third.receiptHandle(),
                        RETRY_DELAY
                )
        );

        assertEquals(1, queue.deadLetterCount());
        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void nackOnFinalAttemptDoesNotCreateDelayedMessage() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        queue.nack(
                first.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(RETRY_DELAY);
        queue.makeDelayedMessagesReady();

        Delivery second = queue.receive().orElseThrow();

        queue.nack(
                second.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(RETRY_DELAY);
        queue.makeDelayedMessagesReady();

        Delivery third = queue.receive().orElseThrow();

        queue.nack(
                third.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(Duration.ofHours(1));

        assertEquals(
                0,
                queue.makeDelayedMessagesReady()
        );

        assertEquals(1, queue.deadLetterCount());
    }

    @Test
    void unknownReceiptHandleCannotBeNacked() {
        assertFalse(
                queue.nack(
                        "unknown-receipt-handle",
                        RETRY_DELAY
                )
        );
    }

    @Test
    void alreadyAcknowledgedDeliveryCannotBeNacked() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        assertTrue(
                queue.ack(delivery.receiptHandle())
        );

        assertFalse(
                queue.nack(
                        delivery.receiptHandle(),
                        RETRY_DELAY
                )
        );
    }

    @Test
    void alreadyNackedDeliveryCannotBeAcknowledged() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        assertTrue(
                queue.nack(
                        delivery.receiptHandle(),
                        RETRY_DELAY
                )
        );

        assertFalse(
                queue.ack(delivery.receiptHandle())
        );
    }

    @Test
    void duplicateNackFails() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        assertTrue(
                queue.nack(
                        delivery.receiptHandle(),
                        RETRY_DELAY
                )
        );

        assertFalse(
                queue.nack(
                        delivery.receiptHandle(),
                        RETRY_DELAY
                )
        );
    }

    @Test
    void expiredReceiptHandleCannotBeNacked() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        clock.advance(VISIBILITY_TIMEOUT);

        queue.requeueExpiredMessages();

        assertFalse(
                queue.nack(
                        first.receiptHandle(),
                        RETRY_DELAY
                )
        );
    }

    @Test
    void staleReceiptHandleCannotNackNewDelivery() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        // First delivery loses ownership.
        clock.advance(VISIBILITY_TIMEOUT);
        queue.requeueExpiredMessages();

        Delivery second = queue.receive().orElseThrow();

        assertNotEquals(
                first.receiptHandle(),
                second.receiptHandle()
        );

        // Old consumer must not be able to affect
        // the new consumer's delivery.
        assertFalse(
                queue.nack(
                        first.receiptHandle(),
                        RETRY_DELAY
                )
        );

        // New owner still owns the active delivery.
        assertTrue(
                queue.ack(second.receiptHandle())
        );
    }

    @Test
    void zeroRetryDelayIsAllowed() {
        queue.publish("A");

        Delivery first = queue.receive().orElseThrow();

        assertTrue(
                queue.nack(
                        first.receiptHandle(),
                        Duration.ZERO
                )
        );

        assertEquals(
                1,
                queue.makeDelayedMessagesReady()
        );

        Delivery second = queue.receive().orElseThrow();

        assertEquals(2, second.attempt());
        assertEquals(
                first.message().id(),
                second.message().id()
        );
    }

    @Test
    void negativeRetryDelayIsRejectedWithoutChangingDeliveryOwnership() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        assertThrows(
                IllegalArgumentException.class,
                () -> queue.nack(
                        delivery.receiptHandle(),
                        Duration.ofSeconds(-1)
                )
        );

        // Validation failure must not remove the
        // currently active delivery.
        assertTrue(
                queue.ack(delivery.receiptHandle())
        );
    }

    @Test
    void delayedMessagesAreAppendedToReadyTail() {
        queue.publish("M1");

        Delivery m1 = queue.receive().orElseThrow();

        queue.nack(
                m1.receiptHandle(),
                RETRY_DELAY
        );

        // M1 is DELAYED.
        // Publish new work while it is delayed.
        queue.publish("M2");
        queue.publish("M3");

        clock.advance(RETRY_DELAY);

        assertEquals(
                1,
                queue.makeDelayedMessagesReady()
        );

        // READY should now be:
        //
        // [M2, M3, M1]
        assertEquals(
                "M2",
                queue.receive().orElseThrow().message().payload()
        );

        assertEquals(
                "M3",
                queue.receive().orElseThrow().message().payload()
        );

        assertEquals(
                "M1",
                queue.receive().orElseThrow().message().payload()
        );
    }

    @Test
    void onlyEligibleDelayedMessagesMoveToReady() {
        queue.publish("M1");

        Delivery m1 = queue.receive().orElseThrow();

        queue.nack(
                m1.receiptHandle(),
                Duration.ofSeconds(5)
        );

        queue.publish("M2");

        Delivery m2 = queue.receive().orElseThrow();

        queue.nack(
                m2.receiptHandle(),
                Duration.ofSeconds(20)
        );

        clock.advance(Duration.ofSeconds(5));

        assertEquals(
                1,
                queue.makeDelayedMessagesReady()
        );

        Delivery redelivery = queue.receive().orElseThrow();

        assertEquals(
                m1.message().id(),
                redelivery.message().id()
        );

        assertTrue(queue.receive().isEmpty());
    }

    @Test
    void repeatedDelayedScanDoesNotDuplicateMessages() {
        queue.publish("A");

        Delivery delivery = queue.receive().orElseThrow();

        queue.nack(
                delivery.receiptHandle(),
                RETRY_DELAY
        );

        clock.advance(RETRY_DELAY);

        assertEquals(
                1,
                queue.makeDelayedMessagesReady()
        );

        assertEquals(
                0,
                queue.makeDelayedMessagesReady()
        );

        Delivery redelivery = queue.receive().orElseThrow();

        assertEquals(
                delivery.message().id(),
                redelivery.message().id()
        );

        assertTrue(queue.receive().isEmpty());
    }
}