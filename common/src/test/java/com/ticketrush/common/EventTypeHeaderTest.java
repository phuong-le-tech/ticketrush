package com.ticketrush.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EventTypeHeaderTest {

    private static final String TYPE_HEADER = "__TypeId__";

    static Stream<Arguments> logicalNames() {
        return Stream.of(
                Arguments.of(Samples.orderRequested(), "OrderRequested"),
                Arguments.of(Samples.ticketsReserved(), "TicketsReserved"),
                Arguments.of(Samples.reservationRejected(), "ReservationRejected"),
                Arguments.of(Samples.paymentSucceeded(), "PaymentResult"),
                Arguments.of(Samples.orderConfirmed(), "OrderConfirmed"),
                Arguments.of(Samples.ticketsReleased(), "TicketsReleased"));
    }

    @ParameterizedTest
    @MethodSource("logicalNames")
    void serializerWritesShortLogicalNameAsTypeHeader(Object event, String expectedName) {
        Headers headers = new RecordHeaders();

        EventJson.serializer().serialize("any.topic", headers, event);

        Header header = headers.lastHeader(TYPE_HEADER);
        assertThat(header).as("type header present").isNotNull();
        String value = new String(header.value(), StandardCharsets.UTF_8);
        assertThat(value).isEqualTo(expectedName);
        assertThat(value).doesNotContain(".").isNotEqualTo(event.getClass().getName());
    }

    @Test
    void messageWithLogicalHeaderDeserializesIntoMatchingRecord() {
        TicketsReserved original = Samples.ticketsReserved();
        byte[] payload = EventJson.mapper().writeValueAsBytes(original);
        Headers headers = new RecordHeaders();
        headers.add(TYPE_HEADER, "TicketsReserved".getBytes(StandardCharsets.UTF_8));

        Object result = EventJson.deserializer().deserialize("any.topic", headers, payload);

        assertThat(result).isInstanceOf(TicketsReserved.class).isEqualTo(original);
    }

    @Test
    void unknownLogicalNameFailsInsteadOfReturningNull() {
        byte[] payload = EventJson.mapper().writeValueAsBytes(Samples.orderRequested());
        Headers headers = new RecordHeaders();
        headers.add(TYPE_HEADER, "Foo".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> EventJson.deserializer().deserialize("any.topic", headers, payload))
                .isInstanceOf(RuntimeException.class);
    }
}
