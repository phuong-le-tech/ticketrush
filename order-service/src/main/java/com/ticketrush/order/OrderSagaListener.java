package com.ticketrush.order;

import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.ReservationRejected;
import com.ticketrush.common.Topics;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consomme les trois topics d'entree. Ordre: ecriture DB (autocommit, un seul upsert), puis send
 * synchrone, puis ack. Limite M1 connue: pas d'outbox, donc un crash entre l'ecriture DB et le send
 * perd l'OrderConfirmed (corrige en M4).
 */
@Component
public class OrderSagaListener {

    private final OrderRepository repository;
    private final KafkaTemplate<String, Object> kafka;

    OrderSagaListener(OrderRepository repository, KafkaTemplate<String, Object> kafka) {
        this.repository = repository;
        this.kafka = kafka;
    }

    @KafkaListener(topics = Topics.ORDERS_REQUESTED)
    void onRequested(OrderRequested event, Acknowledgment ack)
            throws ExecutionException, InterruptedException {
        publishIfConfirmed(event.orderId(), repository.onRequested(event));
        ack.acknowledge();
    }

    @KafkaListener(topics = Topics.PAYMENTS_RESULT)
    void onPayment(PaymentResult event, Acknowledgment ack)
            throws ExecutionException, InterruptedException {
        publishIfConfirmed(event.orderId(), repository.onPayment(event));
        ack.acknowledge();
    }

    @KafkaListener(topics = Topics.ORDERS_REJECTED)
    void onRejected(ReservationRejected event, Acknowledgment ack) {
        repository.onRejected(event);
        ack.acknowledge();
    }

    private void publishIfConfirmed(UUID orderId, Optional<OrderRepository.Saga> transition)
            throws ExecutionException, InterruptedException {
        if (transition.isEmpty() || !transition.get().readyToConfirm()) {
            return;
        }
        OrderRepository.Saga s = transition.get();
        OrderConfirmed confirmed = new OrderConfirmed(UUID.randomUUID(), orderId, s.eventId(),
                s.customerId(), s.quantity(), s.amountCents(), s.currency(), Instant.now());
        kafka.send(Topics.ORDERS_CONFIRMED, orderId.toString(), confirmed).get();
    }
}
