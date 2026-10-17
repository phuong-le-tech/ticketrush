package com.ticketrush.gateway;

import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publie OrderRequested de facon synchrone. On n'accepte la commande (202) qu'une fois le broker
 * ayant confirme l'ecriture (acks=all). Les erreurs et timeouts Kafka sont propagees telles
 * quelles: l'appelant les traduit en 503.
 */
@Component
public class OrderPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final long publishTimeoutMs;

    public OrderPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                          @Value("${gateway.publish-timeout-ms:5000}") long publishTimeoutMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.publishTimeoutMs = publishTimeoutMs;
    }

    public SendResult<String, Object> publish(UUID messageId, UUID orderId, String idempotencyKey,
                                              String eventId, String customerId, int quantity,
                                              Instant occurredAt)
            throws ExecutionException, InterruptedException, TimeoutException {
        OrderRequested event = new OrderRequested(
                messageId, orderId, idempotencyKey, eventId, customerId, quantity, occurredAt);
        // Cle = eventId: toutes les commandes d'un evenement vont sur la meme partition (ordre garanti).
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(Topics.ORDERS_REQUESTED, eventId, event);
        record.headers().add("__TypeId__", "OrderRequested".getBytes(StandardCharsets.UTF_8));
        return kafkaTemplate.send(record).get(publishTimeoutMs, TimeUnit.MILLISECONDS);
    }
}
