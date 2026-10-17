package com.ticketrush.gateway;

import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.Topics;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * POST /orders: valide, publie OrderRequested (cle = eventId) et ne repond 202 qu'apres l'ack de Kafka.
 * Si Kafka n'ack pas dans le delai, 503: on ne promet jamais une commande qui n'est pas dans le log.
 */
@RestController
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final KafkaTemplate<String, Object> kafka;
    private final long publishTimeoutMs;

    OrderController(KafkaTemplate<String, Object> kafka,
                    @Value("${gateway.publish-timeout-ms}") long publishTimeoutMs) {
        this.kafka = kafka;
        this.publishTimeoutMs = publishTimeoutMs;
    }

    @PostMapping("/orders")
    ResponseEntity<Map<String, String>> create(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateOrderRequest request) {
        if (idempotencyKey == null || idempotencyKey.isBlank() || !request.isValid()) {
            return ResponseEntity.badRequest().build();
        }
        UUID orderId = UUID.randomUUID();
        OrderRequested event = new OrderRequested(UUID.randomUUID(), orderId, idempotencyKey,
                request.eventId(), request.customerId(), request.quantity(), Instant.now());
        try {
            kafka.send(Topics.ORDERS_REQUESTED, request.eventId(), event).get(publishTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return unavailable(e);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            return unavailable(e);
        }
        return ResponseEntity.accepted().body(Map.of("orderId", orderId.toString()));
    }

    private static ResponseEntity<Map<String, String>> unavailable(Exception e) {
        log.warn("Kafka publish failed, answering 503: {}", e.toString());
        return ResponseEntity.status(503).build();
    }
}
