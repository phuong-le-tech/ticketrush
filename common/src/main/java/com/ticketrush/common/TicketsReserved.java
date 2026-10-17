package com.ticketrush.common;

import java.time.Instant;
import java.util.UUID;

public record TicketsReserved(UUID messageId, UUID orderId, String eventId, int quantity,
                              long amountCents, String currency, Instant reservedUntil, Instant occurredAt) {
}
