package com.ticketrush.gateway;

/** Corps de POST /orders. quantity est un Integer pour detecter son absence (null) et repondre 400. */
public record CreateOrderRequest(String eventId, String customerId, Integer quantity) {
}
