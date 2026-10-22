package com.ticketrush.gateway;

import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.Topics;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final long sendTimeoutMs;

    OrderController(KafkaTemplate<String, Object> kafkaTemplate,
                    @Value("${gateway.send-timeout-ms}") long sendTimeoutMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.sendTimeoutMs = sendTimeoutMs;
    }

    @PostMapping("/orders")
    ResponseEntity<Map<String, String>> create(@RequestHeader("Idempotency-Key") String idempotencyKey,
                                               @RequestBody CreateOrderRequest request) {
        if (idempotencyKey.isBlank() || !request.isValid()) {
            return ResponseEntity.badRequest().build();
        }
        UUID orderId = UUID.randomUUID();
        OrderRequested event = new OrderRequested(UUID.randomUUID(), orderId, idempotencyKey,
                request.eventId(), request.customerId(), request.quantity(), Instant.now());
        try {
            // Envoi synchrone: on ne repond 202 qu'apres l'ack du broker (acks=all, producer idempotent).
            kafkaTemplate.send(Topics.ORDERS_REQUESTED, request.eventId(), event)
                    .get(sendTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        } catch (Exception e) {
            log.warn("Publication Kafka echouee pour la commande {}: {}", orderId, e.toString());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
        return ResponseEntity.accepted().body(Map.of("orderId", orderId.toString()));
    }
}
