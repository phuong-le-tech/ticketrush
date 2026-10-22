package com.ticketrush.gateway;

public record CreateOrderRequest(String eventId, String customerId, Integer quantity) {

    boolean isValid() {
        return eventId != null && !eventId.isBlank()
                && customerId != null && !customerId.isBlank()
                && quantity != null && quantity >= 1 && quantity <= 4;
    }
}
