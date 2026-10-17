package com.ticketrush.gateway;

import org.springframework.stereotype.Component;

/** Validation de POST /orders. Isolee du chemin de publication Kafka: ne depend de rien d'autre. */
@Component
public class OrderRequestValidator {

    static final int MIN_QUANTITY = 1;
    static final int MAX_QUANTITY = 4;

    /** Leve InvalidOrderRequestException si l'en-tete ou le corps est invalide. */
    public void validate(String idempotencyKey, CreateOrderRequest request) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new InvalidOrderRequestException("Idempotency-Key header is required");
        }
        if (request == null) {
            throw new InvalidOrderRequestException("body is required");
        }
        if (request.eventId() == null || request.eventId().isBlank()) {
            throw new InvalidOrderRequestException("eventId must be a non-empty string");
        }
        if (request.customerId() == null || request.customerId().isBlank()) {
            throw new InvalidOrderRequestException("customerId must be a non-empty string");
        }
        Integer quantity = request.quantity();
        if (quantity == null || quantity < MIN_QUANTITY || quantity > MAX_QUANTITY) {
            throw new InvalidOrderRequestException(
                    "quantity must be an integer between " + MIN_QUANTITY + " and " + MAX_QUANTITY);
        }
    }
}
