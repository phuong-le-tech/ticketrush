package com.ticketrush.common;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Test fixtures: one fully populated instance of each event record.
 *
 * Contract for the code-writer (package com.ticketrush.common, module common):
 * - 6 records exactly as in DESIGN.md section 3, plus enum Status {SUCCEEDED, FAILED}.
 * - Topics: String constants ORDERS_REQUESTED, TICKETS_RESERVED, ORDERS_REJECTED, PAYMENTS_RESULT,
 *   ORDERS_CONFIRMED, TICKETS_RELEASED and int PARTITIONS = 12.
 * - EventJson.mapper() : tools.jackson JsonMapper (Jackson 3) able to (de)serialize all records.
 * - EventJson.serializer() / EventJson.deserializer() : JacksonJsonSerializer / JacksonJsonDeserializer
 *   (Object) with type header "__TypeId__" holding the logical short name (e.g. "OrderRequested").
 */
final class Samples {

    static final long BIG_AMOUNT = 5_000_000_000L;
    static final Instant NANO_INSTANT = Instant.parse("2026-10-17T10:15:30.123456789Z");

    private Samples() {
    }

    static OrderRequested orderRequested() {
        return new OrderRequested(UUID.randomUUID(), UUID.randomUUID(), "key-1", "event-1",
                "customer-1", 4, NANO_INSTANT);
    }

    static TicketsReserved ticketsReserved() {
        return new TicketsReserved(UUID.randomUUID(), UUID.randomUUID(), "event-1", 2,
                BIG_AMOUNT, "EUR", NANO_INSTANT.plusSeconds(300), NANO_INSTANT);
    }

    static ReservationRejected reservationRejected() {
        return new ReservationRejected(UUID.randomUUID(), UUID.randomUUID(), "event-1",
                "SOLD_OUT", NANO_INSTANT);
    }

    static PaymentResult paymentSucceeded() {
        return new PaymentResult(UUID.randomUUID(), UUID.randomUUID(), "event-1", Status.SUCCEEDED,
                BIG_AMOUNT, "EUR", "ch_123", null, NANO_INSTANT);
    }

    static PaymentResult paymentFailed() {
        return new PaymentResult(UUID.randomUUID(), UUID.randomUUID(), "event-1", Status.FAILED,
                BIG_AMOUNT, "EUR", null, "CARD_DECLINED", NANO_INSTANT);
    }

    static PaymentResult paymentFailedWithNulls() {
        return new PaymentResult(UUID.randomUUID(), UUID.randomUUID(), "event-1", Status.FAILED,
                BIG_AMOUNT, "EUR", null, null, NANO_INSTANT);
    }

    static OrderConfirmed orderConfirmed() {
        return new OrderConfirmed(UUID.randomUUID(), UUID.randomUUID(), "event-1", "customer-1",
                3, BIG_AMOUNT, "EUR", NANO_INSTANT);
    }

    static TicketsReleased ticketsReleased() {
        return new TicketsReleased(UUID.randomUUID(), UUID.randomUUID(), "event-1", 2,
                "PAYMENT_FAILED", NANO_INSTANT);
    }

    /** Every event instance, including the nullable-field and FAILED/SUCCEEDED variants. */
    static Stream<Object> allEvents() {
        return Stream.of(orderRequested(), ticketsReserved(), reservationRejected(),
                paymentSucceeded(), paymentFailed(), paymentFailedWithNulls(),
                orderConfirmed(), ticketsReleased());
    }
}
