package com.ticketrush.common;

import java.util.Map;
import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.support.mapping.JacksonJavaTypeMapper.TypePrecedence;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

/**
 * JSON mapper and Kafka serde shared by all services. The "__TypeId__" header carries a short
 * logical name (e.g. OrderRequested), not the Java class name, so renaming a class does not
 * break consumers.
 */
public final class EventJson {

    private static final Map<String, Class<?>> TYPE_MAPPINGS = Map.of(
            "OrderRequested", OrderRequested.class,
            "TicketsReserved", TicketsReserved.class,
            "ReservationRejected", ReservationRejected.class,
            "PaymentResult", PaymentResult.class,
            "OrderConfirmed", OrderConfirmed.class,
            "TicketsReleased", TicketsReleased.class);

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private EventJson() {
    }

    public static JsonMapper mapper() {
        return MAPPER;
    }

    public static JacksonJsonSerializer<Object> serializer() {
        return new JacksonJsonSerializer<Object>(MAPPER).typeMapper(typeMapper());
    }

    public static JacksonJsonDeserializer<Object> deserializer() {
        return new JacksonJsonDeserializer<Object>(Object.class, MAPPER).typeMapper(typeMapper());
    }

    private static DefaultJacksonJavaTypeMapper typeMapper() {
        DefaultJacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();
        typeMapper.setIdClassMapping(TYPE_MAPPINGS);
        typeMapper.setTypePrecedence(TypePrecedence.TYPE_ID);
        return typeMapper;
    }
}
