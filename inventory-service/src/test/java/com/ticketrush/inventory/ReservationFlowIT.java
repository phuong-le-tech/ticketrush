package com.ticketrush.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.ReservationRejected;
import com.ticketrush.common.TicketsReserved;
import com.ticketrush.common.Topics;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Flux complet avec Kafka et Redis reels, sans mock. Contrat de configuration vise (code-writer):
 * - spring.kafka.bootstrap-servers, spring.data.redis.host / port (config Boot standard)
 * - inventory.seed.event-id / stock / price-cents / currency (equivalents de INVENTORY_SEED_*)
 * - inventory.reservation-ttl-seconds (equivalent de RESERVATION_TTL_SECONDS)
 */
@SpringBootTest(properties = {
        "inventory.seed.event-id=seed-event",
        "inventory.seed.stock=7",
        "inventory.seed.price-cents=4200",
        "inventory.seed.currency=EUR",
        "inventory.reservation-ttl-seconds=300",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
class ReservationFlowIT {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:8.10.2").withExposedPorts(6379);

    static {
        KAFKA.start();
        REDIS.start();
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS_REQUESTED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.TICKETS_RESERVED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.ORDERS_REJECTED, Topics.PARTITIONS, (short) 1))).all().get();
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

    private KafkaConsumer<String, Object> reservedConsumer;
    private KafkaConsumer<String, Object> rejectedConsumer;

    @BeforeEach
    void setUp() {
        reservedConsumer = consumer(Topics.TICKETS_RESERVED);
        rejectedConsumer = consumer(Topics.ORDERS_REJECTED);
    }

    @AfterEach
    void tearDown() {
        reservedConsumer.close();
        rejectedConsumer.close();
    }

    private KafkaConsumer<String, Object> consumer(String topic) {
        KafkaConsumer<String, Object> c = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + topic + "-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), EventJson.deserializer());
        c.subscribe(List.of(topic));
        return c;
    }

    private void seedRedis(String eventId, long stock, long priceCents, String currency) {
        String key = "inventory:event:" + eventId;
        redis.delete(key);
        redis.opsForHash().put(key, "stock", String.valueOf(stock));
        redis.opsForHash().put(key, "priceCents", String.valueOf(priceCents));
        redis.opsForHash().put(key, "currency", currency);
    }

    private void send(OrderRequested order) throws Exception {
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                new StringSerializer(), EventJson.serializer())) {
            producer.send(new ProducerRecord<>(Topics.ORDERS_REQUESTED, order.eventId(), order)).get();
        }
    }

    private static List<ConsumerRecord<String, Object>> poll(KafkaConsumer<String, Object> c, Duration timeout) {
        List<ConsumerRecord<String, Object>> out = new ArrayList<>();
        c.poll(timeout).forEach(out::add);
        return out;
    }

    private static OrderRequested order(String eventId, int quantity) {
        return new OrderRequested(UUID.randomUUID(), UUID.randomUUID(), "idem-" + UUID.randomUUID(),
                eventId, "customer-1", quantity, Instant.now());
    }

    @Test
    void seedAtStartupCreatesTheKeyFromConfiguration() {
        assertThat(redis.opsForHash().entries("inventory:event:seed-event"))
                .containsEntry("stock", "7")
                .containsEntry("priceCents", "4200")
                .containsEntry("currency", "EUR");
    }

    @Test
    void orderRequestedProducesTicketsReservedKeyedByOrderIdWithPriceFromRedis() throws Exception {
        seedRedis("flow-ok", 10, 12550, "EUR");
        OrderRequested order = order("flow-ok", 4);

        send(order);

        List<ConsumerRecord<String, Object>> received = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            poll(reservedConsumer, Duration.ofMillis(500)).stream()
                    .filter(r -> r.value() instanceof TicketsReserved t && t.orderId().equals(order.orderId()))
                    .forEach(received::add);
            assertThat(received).hasSize(1);
        });

        ConsumerRecord<String, Object> record = received.get(0);
        TicketsReserved reserved = (TicketsReserved) record.value();
        assertThat(record.key()).isEqualTo(order.orderId().toString());
        assertThat(reserved.eventId()).isEqualTo("flow-ok");
        assertThat(reserved.quantity()).isEqualTo(4);
        assertThat(reserved.amountCents()).isEqualTo(50200L);
        assertThat(reserved.currency()).isEqualTo("EUR");
        assertThat(reserved.reservedUntil()).isAfter(reserved.occurredAt());
        assertThat(reserved.reservedUntil())
                .isBetween(Instant.now().plusSeconds(200), Instant.now().plusSeconds(400));
        assertThat(redis.opsForHash().get("inventory:event:flow-ok", "stock")).isEqualTo("6");
    }

    @Test
    void soldOutProducesReservationRejectedAndNothingIsReserved() throws Exception {
        seedRedis("flow-empty", 0, 12550, "EUR");
        OrderRequested order = order("flow-empty", 1);

        send(order);

        List<ConsumerRecord<String, Object>> rejected = new ArrayList<>();
        List<ConsumerRecord<String, Object>> reserved = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            poll(rejectedConsumer, Duration.ofMillis(500)).stream()
                    .filter(r -> r.value() instanceof ReservationRejected x && x.orderId().equals(order.orderId()))
                    .forEach(rejected::add);
            poll(reservedConsumer, Duration.ofMillis(100)).stream()
                    .filter(r -> r.value() instanceof TicketsReserved t && t.orderId().equals(order.orderId()))
                    .forEach(reserved::add);
            assertThat(rejected).hasSize(1);
        });

        ConsumerRecord<String, Object> record = rejected.get(0);
        ReservationRejected value = (ReservationRejected) record.value();
        assertThat(record.key()).isEqualTo(order.orderId().toString());
        assertThat(value.reason()).isEqualTo("SOLD_OUT");
        assertThat(value.eventId()).isEqualTo("flow-empty");
        assertThat(reserved).isEmpty();
        assertThat(redis.opsForHash().get("inventory:event:flow-empty", "stock")).isEqualTo("0");
    }
}
