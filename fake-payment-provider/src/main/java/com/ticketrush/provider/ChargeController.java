package com.ticketrush.provider;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
 */
@RestController
@RequestMapping("/charges")
public class ChargeController {

    record ChargeRequest(UUID orderId, long amountCents, String currency) {
    }

    record Charge(String chargeId, String status, UUID orderId, long amountCents, String currency) {
    }

    private final Map<String, Charge> chargesByKey = new ConcurrentHashMap<>();

    /** Idempotency-Key absent: Spring repond 400 (header obligatoire) avant toute creation. */
    @PostMapping
    Charge charge(@RequestHeader("Idempotency-Key") String idempotencyKey, @RequestBody ChargeRequest request) {
        return chargesByKey.computeIfAbsent(idempotencyKey, k -> new Charge("ch_" + UUID.randomUUID(),
                "SUCCEEDED", request.orderId(), request.amountCents(), request.currency()));
    }

    /** Sert aux tests pour compter les charges d'une cle. */
    @GetMapping
    List<Charge> charges(@RequestParam("idempotencyKey") String idempotencyKey) {
        Charge charge = chargesByKey.get(idempotencyKey);
        return charge == null ? List.of() : List.of(charge);
    }
}
