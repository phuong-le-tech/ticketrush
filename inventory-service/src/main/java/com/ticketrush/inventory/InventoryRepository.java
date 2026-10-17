package com.ticketrush.inventory;

import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * STUB (test-writer): a implementer par le code-writer.
 * Hash Redis: inventory:event:{eventId}, champs stock, priceCents, currency.
 */
public class InventoryRepository {

    public InventoryRepository(StringRedisTemplate redis) {
    }

    /** Ecrit les 3 champs seulement si la cle n'existe pas (atomique). */
    public void seedIfAbsent(String eventId, long stock, long priceCents, String currency) {
        throw new UnsupportedOperationException("not implemented");
    }

    /** Un seul script Lua: stock >= quantity puis decrement, tout ou rien. Event inconnu = refus. */
    public ReservationResult reserve(String eventId, int quantity) {
        throw new UnsupportedOperationException("not implemented");
    }
}
