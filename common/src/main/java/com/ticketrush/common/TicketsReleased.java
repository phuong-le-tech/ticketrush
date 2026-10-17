package com.ticketrush.common;

import java.time.Instant;
import java.util.UUID;

public record TicketsReleased(UUID messageId, UUID orderId, String eventId, int quantity,
                              String reason, Instant occurredAt) {
}
