package com.ticketrush.provider;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Faux fournisseur de paiement. computeIfAbsent est atomique sur ConcurrentHashMap: deux requetes
 * simultanees avec la meme Idempotency-Key ne creent qu'une seule charge (pas de double facturation).
 * A share of first attempts (fake-provider.failure-rate) is declined. The decline is stored under
 * the key like a charge, so a retry with the same key gets the same answer, as with a real provider.
 */
@RestController
@RequestMapping("/charges")
public class ChargeController {

    static final String SUCCEEDED = "SUCCEEDED";
    static final String FAILED = "FAILED";

    record ChargeRequest(UUID orderId, long amountCents, String currency) {
    }

    /** chargeId is null for a declined attempt: no money moved. */
    record Charge(String chargeId, String status, UUID orderId, long amountCents, String currency) {
    }

    private final Map<String, Charge> chargesByKey = new ConcurrentHashMap<>();
    private final double failureRate;

    ChargeController(@Value("${fake-provider.failure-rate:0}") double failureRate) {
        if (failureRate < 0.0 || failureRate > 1.0) {
            throw new IllegalArgumentException("fake-provider.failure-rate must be between 0.0 and 1.0");
        }
        this.failureRate = failureRate;
    }

    /** Idempotency-Key absent: Spring repond 400 (header obligatoire) avant toute creation. */
    @PostMapping
    Charge charge(@RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody ChargeRequest request) {
        return chargesByKey.computeIfAbsent(idempotencyKey, k -> declined()
                ? new Charge(null, FAILED, request.orderId(), request.amountCents(), request.currency())
                : new Charge("ch_" + UUID.randomUUID(), SUCCEEDED, request.orderId(), request.amountCents(),
                        request.currency()));
    }

    /** Sert aux tests pour compter les charges d'une cle (a decline is not a charge). */
    @GetMapping
    List<Charge> charges(@RequestParam("idempotencyKey") String idempotencyKey) {
        Charge charge = chargesByKey.get(idempotencyKey);
        return charge == null || !SUCCEEDED.equals(charge.status()) ? List.of() : List.of(charge);
    }

    /** nextDouble() is in [0, 1): rate 0 never declines, rate 1 always does. */
    private boolean declined() {
        return ThreadLocalRandom.current().nextDouble() < failureRate;
    }
}
