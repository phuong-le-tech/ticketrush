package com.ticketrush.inventory;

import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Stock dans Redis. Hash inventory:event:{eventId}, champs stock, priceCents, currency.
 * Chaque operation est un script Lua unique: verifier puis decrementer sans fenetre de course.
 */
public class InventoryRepository {

    private static final String KEY_PREFIX = "inventory:event:";

    private final StringRedisTemplate redis;
    private final DefaultRedisScript<Long> seedScript = new DefaultRedisScript<>();
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> reserveScript = new DefaultRedisScript<>();

    public InventoryRepository(StringRedisTemplate redis) {
        this.redis = redis;
        seedScript.setLocation(new ClassPathResource("seed.lua"));
        seedScript.setResultType(Long.class);
        reserveScript.setLocation(new ClassPathResource("reserve.lua"));
        reserveScript.setResultType(List.class);
    }

    /** Ecrit les 3 champs seulement si la cle n'existe pas (atomique). */
    public void seedIfAbsent(String eventId, long stock, long priceCents, String currency) {
        redis.execute(seedScript, List.of(KEY_PREFIX + eventId),
                String.valueOf(stock), String.valueOf(priceCents), currency);
    }

    /** Un seul script Lua: stock >= quantity puis decrement, tout ou rien. Event inconnu = refus. */
    public ReservationResult reserve(String eventId, int quantity) {
        List<?> result = redis.execute(reserveScript, List.of(KEY_PREFIX + eventId),
                String.valueOf(quantity));
        if (result == null || result.isEmpty() || ((Number) result.get(0)).longValue() != 1L) {
            return new ReservationResult(false, 0L, null);
        }
        return new ReservationResult(true, Long.parseLong(String.valueOf(result.get(1))),
                String.valueOf(result.get(2)));
    }
}
