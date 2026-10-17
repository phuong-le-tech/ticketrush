package com.ticketrush.common;

import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.json.JsonMapper;

/** Stub (red step): JSON mapper and Kafka serde with logical type header, to be implemented. */
public final class EventJson {

    private EventJson() {
    }

    public static JsonMapper mapper() {
        throw new UnsupportedOperationException("not implemented");
    }

    public static JacksonJsonSerializer<Object> serializer() {
        throw new UnsupportedOperationException("not implemented");
    }

    public static JacksonJsonDeserializer<Object> deserializer() {
        throw new UnsupportedOperationException("not implemented");
    }
}
