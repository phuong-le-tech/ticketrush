package com.ticketrush.notification;

import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Simule l'envoi d'une notification: une ligne NOTIFY stable, grep-able par les tests e2e. */
@Component
public class OrderConfirmedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderConfirmedListener.class);

    @KafkaListener(topics = Topics.ORDERS_CONFIRMED)
    void onOrderConfirmed(OrderConfirmed order) {
        log.info("NOTIFY orderId={} customerId={} quantity={} amount={} {}",
                order.orderId(), order.customerId(), order.quantity(), order.amountCents(), order.currency());
    }
}
