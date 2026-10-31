package com.ticketrush.common;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.support.mapping.JacksonJavaTypeMapper.TypePrecedence;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
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

    /**
     * Deserializer for listeners: a record that cannot be read (bad JSON, unknown type) reaches the
     * error handler with its raw bytes instead of failing inside poll(), so it can be dead-lettered.
     */
    public static Deserializer<Object> listenerDeserializer() {
        return new ErrorHandlingDeserializer<>(deserializer());
    }

    /**
     * Serializer for service producers: events as JSON, and byte[] passed through untouched so a
     * dead-lettered record that could not be deserialized keeps its original bytes.
     */
    public static Serializer<Object> producerSerializer() {
        Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();
        delegates.put(byte[].class, new ByteArraySerializer());
        delegates.put(Object.class, serializer());
        return new DelegatingByTypeSerializer(delegates, true);
    }

    /** Logical type name of an event class, as written in the "__TypeId__" header. */
    public static String typeIdOf(Class<?> eventClass) {
        return TYPE_MAPPINGS.entrySet().stream().filter(e -> e.getValue().equals(eventClass))
                .map(Map.Entry::getKey).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Not an event class: " + eventClass.getName()));
    }

    /** Event class for a logical type name. */
    public static Class<?> classOf(String typeId) {
        Class<?> eventClass = TYPE_MAPPINGS.get(typeId);
        if (eventClass == null) {
            throw new IllegalArgumentException("Unknown event type: " + typeId);
        }
        return eventClass;
    }

    private static DefaultJacksonJavaTypeMapper typeMapper() {
        DefaultJacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();
        typeMapper.setIdClassMapping(TYPE_MAPPINGS);
        typeMapper.setTypePrecedence(TypePrecedence.TYPE_ID);
        return typeMapper;
    }
}
