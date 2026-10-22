package com.ticketrush.inventory;

import java.time.Instant;

/**
 * Outcome of the reserve Lua script for one order. For a replayed order it is the outcome stored
 * the first time, so quantity and reservedUntil are those of the first processing.
 */
public record ReservationResult(boolean reserved, long priceCents, int quantity, String currency,
                                Instant reservedUntil) {

    static ReservationResult rejected() {
        return new ReservationResult(false, 0L, 0, null, null);
    }
}
