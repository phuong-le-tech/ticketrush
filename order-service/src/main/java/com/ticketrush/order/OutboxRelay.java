package com.ticketrush.order;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;

/** Compile-only M4 contract. No implementation or Spring wiring. */
public class OutboxRelay {
    public OutboxRelay(JdbcClient jdbc, KafkaTemplate<String, Object> kafka) {
        throw new UnsupportedOperationException("M4 outbox relay is not implemented");
    }

    public void publishPending() {
        throw new UnsupportedOperationException("M4 outbox relay is not implemented");
    }
}
