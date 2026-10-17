package com.ticketrush.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PricingTest {

    @Test
    void amountIsPriceTimesQuantityInCents() {
        assertThat(Pricing.amountCents(12550L, 4)).isEqualTo(50200L);
    }

    @Test
    void singleTicketCostsExactlyThePrice() {
        assertThat(Pricing.amountCents(12550L, 1)).isEqualTo(12550L);
    }
}
