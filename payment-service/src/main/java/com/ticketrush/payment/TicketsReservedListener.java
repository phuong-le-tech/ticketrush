package com.ticketrush.payment;

import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.Status;
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
 * Consomme tickets.reserved, debite via le provider, publie payments.result (cle = orderId).
 * A charge or a decline is published. A provider with no usable answer throws, so the shared error
 * handler retries (backoff, same Idempotency-Key = orderId) and then dead-letters the record; the
 * order then times out in order-service and its tickets are released. L'offset n'est valide
 * qu'apres confirmation d'envoi par Kafka: si la publication echoue, le message est rejoue, et le
 * provider dedoublonne grace a Idempotency-Key = orderId.
 */
@Component
public class TicketsReservedListener {

    private final ProviderClient provider;
    private final KafkaTemplate<String, Object> kafka;
    private Timer processing;
    @Value("${ticketrush.consumer.processing-delay-ms:0}")
    private long processingDelayMs;

    @Autowired
    void registerMetrics(MeterRegistry registry) {
        processing = registry.timer("ticketrush.consumer.processing", "consumer", "payment-service",
                "topic", Topics.TICKETS_RESERVED);
    }

    TicketsReservedListener(ProviderClient provider, KafkaTemplate<String, Object> kafka) {
        this.provider = provider;
        this.kafka = kafka;
    }

    @KafkaListener(topics = Topics.TICKETS_RESERVED)
    void onTicketsReserved(TicketsReserved reserved, Acknowledgment ack)
            throws ExecutionException, InterruptedException {
        long started = System.nanoTime();
        try {
            if (processingDelayMs > 0) Thread.sleep(processingDelayMs);
            ProviderClient.Outcome outcome = provider.charge(reserved.orderId(), reserved.amountCents(),
                    reserved.currency());
            PaymentResult result = new PaymentResult(UUID.randomUUID(), reserved.orderId(), reserved.eventId(),
                    outcome.succeeded() ? Status.SUCCEEDED : Status.FAILED, reserved.amountCents(),
                    reserved.currency(), outcome.chargeId(), outcome.failureReason(), Instant.now());
            kafka.send(Topics.PAYMENTS_RESULT, reserved.orderId().toString(), result).get();
            ack.acknowledge();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            if (processing != null) processing.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
        }
    }
}
