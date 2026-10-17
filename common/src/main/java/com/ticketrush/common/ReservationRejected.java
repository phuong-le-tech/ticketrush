package com.ticketrush.common;

import java.time.Instant;
import java.util.UUID;

public record ReservationRejected(UUID messageId, UUID orderId, String eventId,
                                  String reason, Instant occurredAt) {
}
