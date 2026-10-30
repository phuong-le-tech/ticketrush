package com.ticketrush.e2e;

import com.ticketrush.common.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;

/** Test-only observation. Does not supply recovery, retry policy, deserialization or listener logic. */
final class M4DeliverySupport {
    private M4DeliverySupport() { }

    record Delivery(String topic, int partition, long offset) {
        static Delivery of(ConsumerRecord<?, ?> record) {
            return new Delivery(record.topic(), record.partition(), record.offset());
        }
        static Delivery of(RecordMetadata record) {
            return new Delivery(record.topic(), record.partition(), record.offset());
        }
    }

    record Attempt(Delivery delivery, int number, long nanos) { }

    static final class Probe implements RecordInterceptor<Object, Object> {
        private final List<Attempt> failures = new CopyOnWriteArrayList<>();
        private final List<Delivery> successes = new CopyOnWriteArrayList<>();

        static Probe install(ConfigurableApplicationContext context) {
            Probe probe = new Probe();
            KafkaListenerEndpointRegistry registry = context.getBean(KafkaListenerEndpointRegistry.class);
            registry.stop();
            registry.getListenerContainers().stream()
                    .map(container -> (AbstractMessageListenerContainer<?, ?>) container)
                    .forEach(container -> instrument(container, probe));
            registry.start();
            return probe;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        private static void instrument(AbstractMessageListenerContainer container, Probe probe) {
            container.setRecordInterceptor(probe);
            // A missing production handler must remain missing so the DLT assertion exposes it.
            Optional.ofNullable(container.getCommonErrorHandler())
                    .filter(DefaultErrorHandler.class::isInstance)
                    .map(DefaultErrorHandler.class::cast)
                    .ifPresent(handler -> handler.setRetryListeners((record, error, attempt) ->
                            probe.failures.add(new Attempt(Delivery.of(record), attempt, System.nanoTime()))));
        }

        @Override
        public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record,
                                                        Consumer<Object, Object> consumer) {
            return record;
        }

        @Override
        public void success(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
            successes.add(Delivery.of(record));
        }

        List<Attempt> attempts(RecordMetadata record) {
            return failures.stream().filter(attempt -> attempt.delivery().equals(Delivery.of(record))).toList();
        }

        List<Delivery> successes() {
            return List.copyOf(successes);
        }
    }

    static RecordMetadata send(E2EStack stack, String topic, int partition, String key,
                               byte[] payload, String type) throws Exception {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.<String, Object>of(
                "bootstrap.servers", stack.bootstrapServers(), "acks", "all", "enable.idempotence", true),
                new StringSerializer(), new ByteArraySerializer())) {
            return producer.send(new ProducerRecord<>(topic, partition, null, key, payload,
                    List.of(new RecordHeader("__TypeId__", type.getBytes(StandardCharsets.UTF_8))))).get();
        }
    }

    static Object valid(String topic, UUID orderId, String eventId) {
        Instant now = Instant.now();
        return switch (topic) {
            case Topics.ORDERS_REQUESTED -> new OrderRequested(UUID.randomUUID(), orderId,
                    "dlt-valid-" + orderId, eventId, "dlt-customer", 1, now);
            case Topics.TICKETS_RESERVED -> new TicketsReserved(UUID.randomUUID(), orderId, eventId,
                    1, 5000L, "EUR", now.plus(Duration.ofMinutes(5)), now);
            case Topics.PAYMENTS_RESULT -> new PaymentResult(UUID.randomUUID(), orderId, eventId,
                    Status.SUCCEEDED, 5000L, "EUR", "dlt-charge-" + orderId, null, now);
            case Topics.ORDERS_REJECTED -> new ReservationRejected(UUID.randomUUID(), orderId,
                    eventId, "SOLD_OUT", now);
            case Topics.ORDERS_CONFIRMED -> new OrderConfirmed(UUID.randomUUID(), orderId, eventId,
                    "dlt-customer", 1, 5000L, "EUR", now);
            case Topics.TICKETS_RELEASED -> new TicketsReleased(UUID.randomUUID(), orderId, eventId,
                    1, "TIMEOUT", now);
            default -> throw new IllegalArgumentException("Not a consumed source topic: " + topic);
        };
    }

    static byte[] json(Object event) {
        return EventJson.mapper().writeValueAsBytes(event);
    }
}
