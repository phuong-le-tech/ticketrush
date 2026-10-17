package com.ticketrush.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.kafka.KafkaContainer;

/** POST /orders avec Kafka reel (Testcontainers), sans mock de Kafka, du serde ni des UUID. */
@SpringBootTest
@AutoConfigureMockMvc
class OrderPublishIT {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    static {
        KAFKA.start();
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS_REQUESTED, Topics.PARTITIONS, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    MockMvc mockMvc;

    private KafkaConsumer<String, byte[]> consumer;

    @BeforeEach
    void setUp() {
        consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-gateway-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(Topics.ORDERS_REQUESTED));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    /** Envoie un POST valide, verifie le 202 et retourne l'orderId du body. */
    private String postValid(String eventId, String customerId, int quantity, String idempotencyKey)
            throws Exception {
        String body = "{\"eventId\":\"" + eventId + "\",\"customerId\":\"" + customerId
                + "\",\"quantity\":" + quantity + "}";
        String response = mockMvc.perform(post("/orders")
                        .header("Idempotency-Key", idempotencyKey)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted())
                .andReturn().getResponse().getContentAsString();
        String orderId = EventJson.mapper().readTree(response).get("orderId").asString();
        assertThat(UUID.fromString(orderId)).isNotNull();
        return orderId;
    }

    private ConsumerRecord<String, byte[]> awaitRecordFor(String eventId) {
        List<ConsumerRecord<String, byte[]>> found = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                if (eventId.equals(r.key())) {
                    found.add(r);
                }
            });
            assertThat(found).hasSize(1);
        });
        return found.get(0);
    }

    private static OrderRequested decode(ConsumerRecord<String, byte[]> record) {
        return (OrderRequested) EventJson.deserializer()
                .deserialize(Topics.ORDERS_REQUESTED, record.headers(), record.value());
    }

    @Test
    void validPostReturns202AndPublishesOrderRequestedKeyedByEventId() throws Exception {
        String orderId = postValid("pub-event", "customer-7", 2, "key-123");

        ConsumerRecord<String, byte[]> record = awaitRecordFor("pub-event");
        assertThat(record.key()).isEqualTo("pub-event");
        assertThat(record.headers().lastHeader("__TypeId__")).isNotNull();
        assertThat(new String(record.headers().lastHeader("__TypeId__").value(), StandardCharsets.UTF_8))
                .isEqualTo("OrderRequested");

        OrderRequested event = decode(record);
        assertThat(event.orderId()).hasToString(orderId);
        assertThat(event.idempotencyKey()).isEqualTo("key-123");
        assertThat(event.eventId()).isEqualTo("pub-event");
        assertThat(event.customerId()).isEqualTo("customer-7");
        assertThat(event.quantity()).isEqualTo(2);
        assertThat(event.messageId()).isNotNull().isNotEqualTo(event.orderId());
        assertThat(event.occurredAt()).isNotNull();
    }

    @Test
    void quantityFourIsAccepted() throws Exception {
        postValid("pub-event-max", "customer-8", 4, "key-max");

        assertThat(decode(awaitRecordFor("pub-event-max")).quantity()).isEqualTo(4);
    }
}
