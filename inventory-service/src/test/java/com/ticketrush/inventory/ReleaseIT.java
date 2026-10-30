package com.ticketrush.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.TicketsReleased;
import com.ticketrush.common.Topics;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;

/** M4 AC2: real Kafka delivery and Redis state, including committed no-op releases. */
@SpringBootTest(properties = {
        "inventory.seed.event-id=release-seed",
        "inventory.reservation-ttl-seconds=3600",
        "spring.kafka.consumer.group-id=release-it",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@org.junit.jupiter.api.Tag("m4-red")
class ReleaseIT {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:8.10.2").withExposedPorts(6379);

    static {
        KAFKA.start();
        REDIS.start();
        try (AdminClient admin = admin()) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS_REQUESTED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.TICKETS_RESERVED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.ORDERS_REJECTED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.TICKETS_RELEASED, Topics.PARTITIONS, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    StringRedisTemplate redis;

    @ParameterizedTest(name = "AC2 release reason={0}")
    @ValueSource(strings = {"PAYMENT_FAILED", "TIMEOUT"})
    void AC2_unknownRejectedAndRepeatedReleasesNeverIncreaseStock(String reason) throws Exception {
        String eventId = "release-" + UUID.randomUUID();
        String missingEventId = "missing-" + UUID.randomUUID();
        String rejectedEventId = "sold-out-" + UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        UUID rejectedOrderId = UUID.randomUUID();
        seed(eventId, 7);
        seed(rejectedEventId, 0);

        try (KafkaProducer<String, Object> producer = producer(); AdminClient admin = admin()) {
            // An unknown order must not be restocked or marked released before it ever reserves.
            awaitConsumed(admin, sendRelease(producer, orderId, eventId, reason));
            assertStock(eventId, "7");
            assertThat(redis.hasKey("inventory:order:" + orderId)).isFalse();

            UUID missingOrderId = UUID.randomUUID();
            awaitConsumed(admin, sendRelease(producer, missingOrderId, missingEventId, reason));
            assertThat(redis.hasKey("inventory:event:" + missingEventId)).isFalse();
            assertThat(redis.hasKey("inventory:order:" + missingOrderId)).isFalse();

            sendOrder(producer, rejectedOrderId, rejectedEventId);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(redis.opsForHash().get("inventory:order:" + rejectedOrderId, "reserved"))
                            .isEqualTo("0"));
            Map<Object, Object> rejected = redis.opsForHash().entries("inventory:order:" + rejectedOrderId);
            awaitConsumed(admin, sendRelease(producer, rejectedOrderId, rejectedEventId, reason));
            assertStock(rejectedEventId, "0");
            assertThat(redis.opsForHash().entries("inventory:order:" + rejectedOrderId)).isEqualTo(rejected);

            sendOrder(producer, orderId, eventId);
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertStock(eventId, "5");
                assertThat(redis.opsForHash().get("inventory:order:" + orderId, "reserved")).isEqualTo("1");
            });

            TicketsReleased release = release(orderId, eventId, reason);
            awaitConsumed(admin, sendRelease(producer, release));
            assertStock(eventId, "7");

            // Both a byte-identical replay and a new messageId for the same order must be no-ops.
            awaitConsumed(admin, sendRelease(producer, release));
            assertStock(eventId, "7");
            String otherReason = "TIMEOUT".equals(reason) ? "PAYMENT_FAILED" : "TIMEOUT";
            awaitConsumed(admin, sendRelease(producer, orderId, eventId, otherReason));
            assertStock(eventId, "7");
            assertStock(rejectedEventId, "0");
            assertThat(redis.hasKey("inventory:event:" + missingEventId)).isFalse();
        }
    }

    private void seed(String eventId, int stock) {
        redis.opsForHash().putAll("inventory:event:" + eventId,
                Map.of("stock", String.valueOf(stock), "priceCents", "4200", "currency", "EUR"));
    }

    private void assertStock(String eventId, String expected) {
        assertThat(redis.opsForHash().get("inventory:event:" + eventId, "stock"))
                .as("stock for %s", eventId).isEqualTo(expected);
    }

    private static AdminClient admin() {
        return AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    }

    private static KafkaProducer<String, Object> producer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"), new StringSerializer(), EventJson.serializer());
    }

    private static void sendOrder(KafkaProducer<String, Object> producer, UUID orderId, String eventId)
            throws Exception {
        OrderRequested order = new OrderRequested(UUID.randomUUID(), orderId, orderId.toString(),
                eventId, "release-customer", 2, Instant.now());
        producer.send(new ProducerRecord<>(Topics.ORDERS_REQUESTED, eventId, order))
                .get(30, TimeUnit.SECONDS);
    }

    private static TicketsReleased release(UUID orderId, String eventId, String reason) {
        return new TicketsReleased(UUID.randomUUID(), orderId, eventId, 2, reason, Instant.now());
    }

    private static RecordMetadata sendRelease(KafkaProducer<String, Object> producer, UUID orderId,
                                              String eventId, String reason) throws Exception {
        return sendRelease(producer, release(orderId, eventId, reason));
    }

    private static RecordMetadata sendRelease(KafkaProducer<String, Object> producer, TicketsReleased release)
            throws Exception {
        return producer.send(new ProducerRecord<>(Topics.TICKETS_RELEASED, release.eventId(), release))
                .get(30, TimeUnit.SECONDS);
    }

    private static void awaitConsumed(AdminClient admin, RecordMetadata sent) {
        TopicPartition partition = new TopicPartition(sent.topic(), sent.partition());
        // Unchanged stock alone is not evidence: wait for this exact release's committed offset.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            OffsetAndMetadata committed = admin.listConsumerGroupOffsets("release-it")
                    .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(partition);
            assertThat(committed).as("AC2 inventory committed release %s offset %s", partition, sent.offset())
                    .isNotNull();
            assertThat(committed.offset()).isGreaterThanOrEqualTo(sent.offset() + 1);
        });
    }
}
