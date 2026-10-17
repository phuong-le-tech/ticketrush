package com.ticketrush.common;

import java.time.Instant;
import java.util.UUID;

public record PaymentResult(UUID messageId, UUID orderId, String eventId, Status status,
                            long amountCents, String currency, String providerChargeId,
                            String failureReason, Instant occurredAt) {
}
