package com.ticketrush.order;

import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.ReservationRejected;
import com.ticketrush.common.Status;
import com.ticketrush.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consomme les trois topics d'entree. Each message is one DB transaction (status change plus its
 * outbox row), then ack. Publishing is the OutboxRelay's job, so a crash after the commit loses
 * nothing: the outbox row is still there.
 */
@Component
public class OrderSagaListener {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaListener.class);

    private final OrderRepository repository;

    OrderSagaListener(OrderRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = Topics.ORDERS_REQUESTED)
    void onRequested(OrderRequested event, Acknowledgment ack) {
        repository.onRequested(event);
        ack.acknowledge();
    }

    @KafkaListener(topics = Topics.PAYMENTS_RESULT)
    void onPayment(PaymentResult event, Acknowledgment ack) {
        if (repository.onPayment(event).isEmpty() && event.status() == Status.SUCCEEDED) {
            warnIfChargedAfterFailure(event);
        }
        ack.acknowledge();
    }

    @KafkaListener(topics = Topics.ORDERS_REJECTED)
    void onRejected(ReservationRejected event, Acknowledgment ack) {
        repository.onRejected(event);
        ack.acknowledge();
    }

    /**
     * Known M4 limitation: a charge that succeeds after the order already FAILED (saga timeout) is
     * not confirmed, its tickets are already released. The PaymentResult stays on payments.result
     * and this log names the charge, for a refund (not built yet).
     */
    private void warnIfChargedAfterFailure(PaymentResult event) {
        if (repository.statusOf(event.orderId()).filter(OrderRepository.FAILED::equals).isPresent()) {
            log.warn("Charge {} succeeded for order {} after the order FAILED; needs a refund",
                    event.providerChargeId(), event.orderId());
        }
    }
}
