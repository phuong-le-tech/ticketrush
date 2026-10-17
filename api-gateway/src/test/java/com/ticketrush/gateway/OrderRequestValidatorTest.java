package com.ticketrush.gateway;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OrderRequestValidatorTest {

    private final OrderRequestValidator validator = new OrderRequestValidator();

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4})
    void validRequestPasses(int quantity) {
        assertThatCode(() -> validator.validate("k", new CreateOrderRequest("e", "c", quantity)))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 5, 100})
    void quantityOutOfRangeIsRejected(int quantity) {
        assertThatThrownBy(() -> validator.validate("k", new CreateOrderRequest("e", "c", quantity)))
                .isInstanceOf(InvalidOrderRequestException.class);
    }

    @Test
    void missingFieldsAreRejected() {
        assertThatThrownBy(() -> validator.validate(null, new CreateOrderRequest("e", "c", 1)))
                .isInstanceOf(InvalidOrderRequestException.class);
        assertThatThrownBy(() -> validator.validate(" ", new CreateOrderRequest("e", "c", 1)))
                .isInstanceOf(InvalidOrderRequestException.class);
        assertThatThrownBy(() -> validator.validate("k", new CreateOrderRequest(null, "c", 1)))
                .isInstanceOf(InvalidOrderRequestException.class);
        assertThatThrownBy(() -> validator.validate("k", new CreateOrderRequest("e", "", 1)))
                .isInstanceOf(InvalidOrderRequestException.class);
        assertThatThrownBy(() -> validator.validate("k", new CreateOrderRequest("e", "c", null)))
                .isInstanceOf(InvalidOrderRequestException.class);
    }
}
