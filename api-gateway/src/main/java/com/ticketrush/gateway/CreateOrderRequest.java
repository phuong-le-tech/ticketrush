package com.ticketrush.gateway;

/** Corps de POST /orders. quantity est un Integer pour detecter l'absence (null) et repondre 400. */
public record CreateOrderRequest(String eventId, String customerId, Integer quantity) {

    static final int MAX_QUANTITY = 4;

    boolean isValid() {
        return eventId != null && !eventId.isBlank()
                && customerId != null && !customerId.isBlank()
                && quantity != null && quantity >= 1 && quantity <= MAX_QUANTITY;
    }
}
