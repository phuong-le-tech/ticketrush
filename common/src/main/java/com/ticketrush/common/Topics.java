package com.ticketrush.common;

/** Kafka topic names and partition count (DESIGN.md section 4). */
public final class Topics {
    public static final String ORDERS_REQUESTED = "orders.requested";
    public static final String TICKETS_RESERVED = "tickets.reserved";
    public static final String ORDERS_REJECTED = "orders.rejected";
    public static final String PAYMENTS_RESULT = "payments.result";
    public static final String ORDERS_CONFIRMED = "orders.confirmed";
    public static final String TICKETS_RELEASED = "tickets.released";
    public static final int PARTITIONS = 12;

    private Topics() {
    }
}
