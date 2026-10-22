package com.ticketrush.inventory;

/** Calcul du montant d'une commande, en cents (jamais de double). */
public final class Pricing {

    private Pricing() {
    }

    /** multiplyExact: un depassement de long leve une exception au lieu de facturer un montant faux. */
    public static long amountCents(long priceCents, int quantity) {
        return Math.multiplyExact(priceCents, (long) quantity);
    }
}
