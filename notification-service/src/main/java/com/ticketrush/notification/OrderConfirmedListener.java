package com.ticketrush.notification;

import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.Topics;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Simule l'envoi d'une notification: une ligne NOTIFY stable, grep-able par les tests e2e.
 * The order-service outbox is at-least-once, so the same OrderConfirmed can arrive twice: one
 * notification per orderId.
 */
@Component
public class OrderConfirmedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderConfirmedListener.class);
    private static final int MAX_REMEMBERED_ORDERS = 100_000;

    // shortcut: in memory and per pod, so a redelivery after a restart or a rebalance to another pod
    // notifies twice; move to a shared store (Redis or a table) once a notification has a real effect.
    private final Set<UUID> notified = Collections.newSetFromMap(Collections.synchronizedMap(
            new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<UUID, Boolean> eldest) {
                    return size() > MAX_REMEMBERED_ORDERS;
                }
            }));

    @KafkaListener(topics = Topics.ORDERS_CONFIRMED)
    void onOrderConfirmed(OrderConfirmed order) {
        if (!notified.add(order.orderId())) {
            log.debug("Duplicate OrderConfirmed for order {} ignored", order.orderId());
            return;
        }
        log.info("NOTIFY orderId={} customerId={} quantity={} amount={} {}",
                order.orderId(), order.customerId(), order.quantity(), order.amountCents(), order.currency());
    }
}
