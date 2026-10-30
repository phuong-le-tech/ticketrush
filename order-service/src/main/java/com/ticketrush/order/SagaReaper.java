package com.ticketrush.order;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Saga timeout. An order still PENDING or RESERVED more than the timeout after it was created
 * (strictly older) becomes FAILED(TIMEOUT) and its TicketsReleased goes through the outbox. The
 * update is conditional, so an order a payment already settled is never reaped, and a reaped
 * order is never confirmed later. The timeout must exceed the worst case payment time (provider
 * timeout x retry attempts plus backoff), or a slow but good payment loses the race.
 */
public class SagaReaper {

    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(120);
    static final int BATCH_SIZE = 500;

    private static final Logger log = LoggerFactory.getLogger(SagaReaper.class);

    private final OrderRepository repository;
    private final Clock clock;
    private final Duration timeout;

    public SagaReaper(OrderRepository repository, Clock clock) {
        this(repository, clock, DEFAULT_TIMEOUT);
    }

    public SagaReaper(OrderRepository repository, Clock clock, Duration timeout) {
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("saga timeout must be positive");
        }
        this.repository = repository;
        this.clock = clock;
        this.timeout = timeout;
    }

    /** Expires one batch of overdue orders. */
    @Scheduled(fixedDelayString = "${order.saga.reaper-interval-ms:1000}")
    public void reapExpired() {
        Instant now = clock.instant();
        int expired = repository.expire(now.minus(timeout), BATCH_SIZE, now);
        if (expired > 0) {
            log.info("Saga timeout: {} orders FAILED(TIMEOUT), tickets released", expired);
        }
    }
}
