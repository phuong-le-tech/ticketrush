package com.ticketrush.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.Topics;
import com.ticketrush.inventory.InventoryApplication;
import com.ticketrush.notification.NotificationApplication;
import com.ticketrush.order.OrderServiceApplication;
import com.ticketrush.payment.PaymentApplication;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.kafka.support.KafkaHeaders;

/** Every source and consuming group is a separate acceptance case, including shared sources. */
@org.junit.jupiter.api.Tag("m4-red")
class DltE2ETest {
    private static final Duration DEADLINE = Duration.ofSeconds(20);
    private E2EStack stack;
    private Map<String, M4DeliverySupport.Probe> probes;

    record Route(String topic, String group) { }
    enum Variant { MALFORMED_JSON, INVALID_TYPE_HEADER, WELL_FORMED_POISON }

    // A deserialization poison can block every partition assigned to that consumer thread.
    // Fresh brokers and groups per invocation keep a missing DLT from contaminating later cases.
    @BeforeEach
    void startStack() throws Exception {
        stack = E2EStack.start("dlt-seed", 100, Map.of());
        probes = Map.of(
                "inventory-service", M4DeliverySupport.Probe.install(stack.context(InventoryApplication.class)),
                "order-service", M4DeliverySupport.Probe.install(stack.context(OrderServiceApplication.class)),
                "payment-service", M4DeliverySupport.Probe.install(stack.context(PaymentApplication.class)),
                "notification-service", M4DeliverySupport.Probe.install(stack.context(NotificationApplication.class)));
    }

    @AfterEach
    void stopStack() {
        if (stack != null) {
            stack.close();
        }
    }

    static Stream<Arguments> routesAndVariants() {
        return Stream.of(
                new Route(Topics.ORDERS_REQUESTED, "inventory-service"),
                new Route(Topics.ORDERS_REQUESTED, "order-service"),
                new Route(Topics.TICKETS_RESERVED, "payment-service"),
                new Route(Topics.PAYMENTS_RESULT, "order-service"),
                new Route(Topics.ORDERS_REJECTED, "order-service"),
                new Route(Topics.ORDERS_CONFIRMED, "notification-service"),
                new Route(Topics.TICKETS_RELEASED, "inventory-service"))
                .flatMap(route -> Stream.of(Variant.values()).map(variant -> Arguments.of(route, variant)));
    }

    @ParameterizedTest(name = "{0} - {1}")
    @MethodSource("routesAndVariants")
    void AC5_eachConsumerMalformedJsonInvalidTypeAndPoisonReachSamePartitionDLTThenValidSucceeds(
            Route route, Variant variant) throws Exception {
        int partition = 7;
        UUID orderId = UUID.randomUUID();
        String eventId = "dlt-event-" + orderId;
        stack.seedStock(eventId, 10);
        Object valid = M4DeliverySupport.valid(route.topic(), orderId, eventId);
        String validType = valid.getClass().getSimpleName();
        String poisonTopic = switch (route.topic()) {
            case Topics.ORDERS_CONFIRMED -> Topics.ORDERS_REQUESTED;
            default -> Topics.ORDERS_CONFIRMED;
        };
        // The poison is valid JSON and a known event type, but not the listener's accepted schema.
        Object poison = M4DeliverySupport.valid(poisonTopic, UUID.randomUUID(), eventId);
        byte[] badBytes = switch (variant) {
            case MALFORMED_JSON -> "{\"orderId\":".getBytes(StandardCharsets.UTF_8);
            case INVALID_TYPE_HEADER -> M4DeliverySupport.json(valid);
            case WELL_FORMED_POISON -> M4DeliverySupport.json(poison);
        };
        String badType = switch (variant) {
            case MALFORMED_JSON -> validType;
            case INVALID_TYPE_HEADER -> "UnknownM4EventType";
            case WELL_FORMED_POISON -> poison.getClass().getSimpleName();
        };
        String key = switch (route.topic()) {
            case Topics.ORDERS_REQUESTED, Topics.TICKETS_RELEASED -> eventId;
            default -> orderId.toString();
        };
        TopicPartition dltPartition = new TopicPartition(route.topic() + ".DLT", partition);
        List<ConsumerRecord<String, byte[]>> received = new ArrayList<>();
        try (KafkaConsumer<String, byte[]> dlt = new KafkaConsumer<>(Map.<String, Object>of(
                "bootstrap.servers", stack.bootstrapServers(), "group.id", "m4-dlt-" + UUID.randomUUID(),
                "enable.auto.commit", false), new StringDeserializer(), new ByteArrayDeserializer());
             AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", stack.bootstrapServers()))) {
            dlt.assign(List.of(dltPartition));
            dlt.seekToEnd(List.of(dltPartition));
            dlt.position(dltPartition);
            RecordMetadata bad = M4DeliverySupport.send(stack, route.topic(), partition, key, badBytes, badType);
            RecordMetadata good = M4DeliverySupport.send(stack, route.topic(), partition, key,
                    M4DeliverySupport.json(valid), validType);

            await().alias("missing same-partition DLT for " + route + " / " + variant)
                    .atMost(DEADLINE).untilAsserted(() -> {
                        dlt.poll(Duration.ofMillis(100)).forEach(received::add);
                        assertThat(received.stream().filter(record -> key.equals(record.key()))
                                .filter(record -> route.group().equals(groupOf(record))))
                                .as("DLT from the target consumer group, not another subscriber").hasSize(1);
                    });
            ConsumerRecord<String, byte[]> dead = received.stream()
                    .filter(record -> key.equals(record.key()))
                    .filter(record -> route.group().equals(groupOf(record))).findFirst().orElseThrow();
            assertThat(dead.topic()).isEqualTo(route.topic() + ".DLT");
            assertThat(dead.partition()).isEqualTo(bad.partition());
            assertThat(dead.key()).isEqualTo(key);
            assertThat(dead.value()).as("original raw bytes, including malformed JSON").isEqualTo(badBytes);
            assertThat(textHeader(dead, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(route.topic());
            assertThat(ByteBuffer.wrap(requiredHeader(dead, KafkaHeaders.DLT_ORIGINAL_PARTITION)).getInt())
                    .isEqualTo(partition);
            assertThat(ByteBuffer.wrap(requiredHeader(dead, KafkaHeaders.DLT_ORIGINAL_OFFSET)).getLong())
                    .isEqualTo(bad.offset());

            // The approved policy retries malformed/deserialization failures too. Production
            // must override Spring's default fatal classification; the probe does not do so.
            List<M4DeliverySupport.Attempt> attempts = probes.get(route.group()).attempts(bad);
            assertThat(attempts).extracting(M4DeliverySupport.Attempt::number).containsExactly(1, 2, 3, 4);
            assertThat(Duration.ofNanos(attempts.get(1).nanos() - attempts.get(0).nanos()))
                    .as("first backoff").isBetween(Duration.ofMillis(900), Duration.ofMillis(1800));
            assertThat(Duration.ofNanos(attempts.get(2).nanos() - attempts.get(1).nanos()))
                    .as("second backoff").isBetween(Duration.ofMillis(1900), Duration.ofMillis(2800));
            assertThat(Duration.ofNanos(attempts.get(3).nanos() - attempts.get(2).nanos()))
                    .as("third backoff").isBetween(Duration.ofMillis(3900), Duration.ofMillis(4800));
            await().alias("valid record behind poison processed and committed by " + route.group())
                    .atMost(DEADLINE).untilAsserted(() -> {
                        assertThat(probes.get(route.group()).successes()).contains(M4DeliverySupport.Delivery.of(good));
                        var committed = admin.listConsumerGroupOffsets(route.group())
                                .partitionsToOffsetAndMetadata().get();
                        assertThat(committed).containsKey(new TopicPartition(route.topic(), partition));
                        assertThat(committed.get(new TopicPartition(route.topic(), partition)).offset())
                                .isGreaterThan(good.offset());
                    });
        }
    }

    private static String groupOf(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP);
        return java.util.Optional.ofNullable(header).map(Header::value)
                .map(value -> new String(value, StandardCharsets.UTF_8)).orElse("");
    }

    private static byte[] requiredHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertThat(header).as("DLT header %s", name).isNotNull();
        return header.value();
    }

    private static String textHeader(ConsumerRecord<?, ?> record, String name) {
        return new String(requiredHeader(record, name), StandardCharsets.UTF_8);
    }
}
