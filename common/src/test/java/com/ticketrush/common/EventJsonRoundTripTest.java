package com.ticketrush.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class EventJsonRoundTripTest {

    static Stream<Object> events() {
        return Samples.allEvents();
    }

    @ParameterizedTest
    @MethodSource("events")
    void roundTripKeepsEveryField(Object event) {
        JsonMapper mapper = EventJson.mapper();

        String json = mapper.writeValueAsString(event);
        Object back = mapper.readValue(json, event.getClass());

        assertThat(back).isEqualTo(event);
    }

    @Test
    void amountAboveIntRangeIsWrittenAsPlainIntegerAndSurvives() {
        JsonMapper mapper = EventJson.mapper();
        TicketsReserved event = Samples.ticketsReserved();

        String json = mapper.writeValueAsString(event);
        JsonNode amount = mapper.readTree(json).get("amountCents");

        assertThat(amount.isIntegralNumber()).isTrue();
        assertThat(amount.asLong()).isEqualTo(Samples.BIG_AMOUNT);
        assertThat(json).contains("\"amountCents\":5000000000");
        assertThat(mapper.readValue(json, TicketsReserved.class).amountCents())
                .isEqualTo(Samples.BIG_AMOUNT);
    }

    @Test
    void nanosecondInstantPrecisionIsKept() {
        JsonMapper mapper = EventJson.mapper();
        OrderRequested event = Samples.orderRequested();

        OrderRequested back = mapper.readValue(mapper.writeValueAsString(event), OrderRequested.class);

        assertThat(back.occurredAt()).isEqualTo(Samples.NANO_INSTANT);
        assertThat(back.occurredAt().getNano()).isEqualTo(123_456_789);
    }

    @Test
    void failedPaymentWithNullChargeIdAndReasonRoundTrips() {
        JsonMapper mapper = EventJson.mapper();
        PaymentResult event = Samples.paymentFailedWithNulls();

        PaymentResult back = mapper.readValue(mapper.writeValueAsString(event), PaymentResult.class);

        assertThat(back.providerChargeId()).isNull();
        assertThat(back.failureReason()).isNull();
        assertThat(back.status()).isEqualTo(Status.FAILED);
        assertThat(back).isEqualTo(event);
    }
}
