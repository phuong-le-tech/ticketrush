package com.ticketrush.common;

import java.time.Instant;
import java.util.UUID;

public record OrderConfirmed(UUID messageId, UUID orderId, String eventId, String customerId,
                             int quantity, long amountCents, String currency, Instant occurredAt) {
}
