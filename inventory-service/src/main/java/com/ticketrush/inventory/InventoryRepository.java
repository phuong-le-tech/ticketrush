package com.ticketrush.inventory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

/**
 * Stock dans Redis. Hash inventory:event:{eventId}, champs stock, priceCents, currency.
 * Chaque operation est un script Lua unique: verifier puis decrementer sans fenetre de course.
 * Hash inventory:order:{orderId}: outcome of the first reservation attempt for that order (dedupe).
 */
public class InventoryRepository {

    private static final String EVENT_KEY_PREFIX = "inventory:event:";
    private static final String ORDER_KEY_PREFIX = "inventory:order:";
    private static final long RESERVED = 1L;

    private final StringRedisTemplate redis;
    private final Duration dedupTtl;
    private final DefaultRedisScript<Long> seedScript = new DefaultRedisScript<>();
    @SuppressWarnings("rawtypes")
    private final DefaultRedisScript<List> reserveScript = new DefaultRedisScript<>();

    /** dedupTtl: how long an order's outcome is remembered, so replays within it never re-reserve. */
    public InventoryRepository(StringRedisTemplate redis, Duration dedupTtl) {
        this.redis = redis;
        this.dedupTtl = dedupTtl;
        seedScript.setLocation(new ClassPathResource("seed.lua"));
        seedScript.setResultType(Long.class);
        reserveScript.setLocation(new ClassPathResource("reserve.lua"));
        reserveScript.setResultType(List.class);
    }

    /** Ecrit les 3 champs seulement si la cle n'existe pas (atomique). */
    public void seedIfAbsent(String eventId, long stock, long priceCents, String currency) {
        redis.execute(seedScript, List.of(EVENT_KEY_PREFIX + eventId),
                String.valueOf(stock), String.valueOf(priceCents), currency);
    }

    /**
     * One Lua script: if this order was already processed, return its stored outcome without
     * touching stock; otherwise stock >= quantity then decrement (all or nothing) and store the
     * outcome. reservedUntil is only used the first time. Unknown event = rejected, nothing stored.
     */
    public ReservationResult reserve(String eventId, UUID orderId, int quantity, Instant reservedUntil) {
        List<?> result = redis.execute(reserveScript,
                List.of(EVENT_KEY_PREFIX + eventId, ORDER_KEY_PREFIX + orderId),
                String.valueOf(quantity), String.valueOf(reservedUntil.toEpochMilli()),
                String.valueOf(dedupTtl.toSeconds()));
        if (result == null || result.isEmpty() || ((Number) result.get(0)).longValue() != RESERVED) {
            return ReservationResult.rejected();
        }
        return new ReservationResult(true, Long.parseLong(String.valueOf(result.get(1))),
                Integer.parseInt(String.valueOf(result.get(2))), String.valueOf(result.get(3)),
                Instant.ofEpochMilli(Long.parseLong(String.valueOf(result.get(4)))));
    }
}
