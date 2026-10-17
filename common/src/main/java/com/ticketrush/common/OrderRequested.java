package com.ticketrush.common;

import java.time.Instant;
import java.util.UUID;

public record OrderRequested(UUID messageId, UUID orderId, String idempotencyKey, String eventId,
                             String customerId, int quantity, Instant occurredAt) {
}
