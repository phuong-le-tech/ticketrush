package com.ticketrush.inventory;

import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.ReservationRejected;
import com.ticketrush.common.TicketsReserved;
import com.ticketrush.common.Topics;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consomme orders.requested, reserve le stock dans Redis, publie le resultat (cle = orderId).
 * L'offset n'est valide qu'apres confirmation d'envoi par Kafka: en cas d'echec le message est rejoue.
 * A replayed or duplicate order gets its stored outcome from Redis and that outcome is published
 * again: this covers a crash between the Lua script and the send, and downstream consumers already
 * ignore the extra copy (provider key = orderId, conditional upserts in order-service).
 */
@Component
public class OrderRequestedListener {

    static final String REASON_SOLD_OUT = "SOLD_OUT";

    private final InventoryRepository repository;
    private final KafkaTemplate<String, Object> kafka;
    private final long reservationTtlSeconds;
    private Timer processing;
    @Value("${ticketrush.consumer.processing-delay-ms:0}")
    private long processingDelayMs;

    @Autowired
    void registerMetrics(MeterRegistry registry) {
        processing = registry.timer("ticketrush.consumer.processing", "consumer", "inventory-service",
                "topic", Topics.ORDERS_REQUESTED);
    }

    OrderRequestedListener(InventoryRepository repository, KafkaTemplate<String, Object> kafka,
                           @Value("${inventory.reservation-ttl-seconds}") long reservationTtlSeconds) {
        this.repository = repository;
        this.kafka = kafka;
        this.reservationTtlSeconds = reservationTtlSeconds;
    }

    @KafkaListener(topics = Topics.ORDERS_REQUESTED)
    void onOrderRequested(OrderRequested order, Acknowledgment ack)
            throws ExecutionException, InterruptedException {
        long started = System.nanoTime();
        try {
            if (processingDelayMs > 0) Thread.sleep(processingDelayMs);
            Instant now = Instant.now();
            ReservationResult result = repository.reserve(order.eventId(), order.orderId(), order.quantity(),
                    now.plusSeconds(reservationTtlSeconds));
            String key = order.orderId().toString();
            if (result.reserved()) {
                TicketsReserved event = new TicketsReserved(UUID.randomUUID(), order.orderId(), order.eventId(),
                        result.quantity(), Pricing.amountCents(result.priceCents(), result.quantity()),
                        result.currency(), result.reservedUntil(), now);
                kafka.send(Topics.TICKETS_RESERVED, key, event).get();
            } else {
                ReservationRejected event = new ReservationRejected(UUID.randomUUID(), order.orderId(),
                        order.eventId(), REASON_SOLD_OUT, now);
                kafka.send(Topics.ORDERS_REJECTED, key, event).get();
            }
            ack.acknowledge();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            if (processing != null) processing.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
        }
    }
}
