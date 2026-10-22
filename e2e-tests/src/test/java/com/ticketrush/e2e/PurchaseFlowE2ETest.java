package com.ticketrush.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.Topics;
import com.ticketrush.gateway.GatewayApplication;
import com.ticketrush.inventory.InventoryApplication;
import com.ticketrush.notification.NotificationApplication;
import com.ticketrush.order.OrderServiceApplication;
import com.ticketrush.payment.PaymentApplication;
import com.ticketrush.provider.ProviderApplication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Milestone 1 acceptance: one purchase goes through every service and ends as OrderConfirmed.
 * Real Kafka, Postgres and Redis (Testcontainers); each service's Spring context runs in this JVM
 * with its own configuration. Nothing is mocked.
 */
class PurchaseFlowE2ETest {

    private static final String EVENT_ID = "concert-1";
    private static final String ORDER_GROUP = "order-service";

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("..", "deploy", "compose", "init-schemas.sql")
                            .toAbsolutePath().normalize()),
                    "/docker-entrypoint-initdb.d/init-schemas.sql");
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:8.10.2").withExposedPorts(6379);

    private static final List<ConfigurableApplicationContext> CONTEXTS = new ArrayList<>();
    private static int gatewayPort;

    @BeforeAll
    static void startInfrastructureAndServices() throws Exception {
        KAFKA.start();
        POSTGRES.start();
        REDIS.start();
        createTopics();

        // Each context ignores the shared classpath application.yml (several modules ship one) and
        // gets its full configuration here.
        ConfigurableApplicationContext provider = start(ProviderApplication.class, true, Map.of());
        String providerUrl = "http://localhost:" + provider.getEnvironment().getProperty("local.server.port");

        start(OrderServiceApplication.class, false, Map.of(
                "spring.datasource.url", POSTGRES.getJdbcUrl(),
                "spring.datasource.username", POSTGRES.getUsername(),
                "spring.datasource.password", POSTGRES.getPassword(),
                "spring.flyway.schemas", "orders",
                "spring.flyway.default-schema", "orders",
                "spring.kafka.consumer.group-id", ORDER_GROUP,
                "spring.kafka.consumer.enable-auto-commit", "false",
                "spring.kafka.listener.ack-mode", "manual"));
        start(InventoryApplication.class, false, Map.of(
                "spring.data.redis.host", REDIS.getHost(),
                "spring.data.redis.port", String.valueOf(REDIS.getMappedPort(6379)),
                "spring.kafka.consumer.group-id", "inventory-service",
                "spring.kafka.consumer.enable-auto-commit", "false",
                "spring.kafka.listener.ack-mode", "manual",
                "inventory.seed.event-id", EVENT_ID,
                "inventory.seed.stock", "10",
                "inventory.seed.price-cents", "5000",
                "inventory.seed.currency", "EUR",
                "inventory.reservation-ttl-seconds", "300"));
        start(PaymentApplication.class, true, Map.of(
                "spring.kafka.consumer.group-id", "payment-service",
                "spring.kafka.consumer.enable-auto-commit", "false",
                "spring.kafka.listener.ack-mode", "manual",
                "payment.provider.url", providerUrl,
                "payment.provider.timeout-ms", "2000"));
        start(NotificationApplication.class, false, Map.of(
                "spring.kafka.consumer.group-id", "notification-service",
                "spring.kafka.listener.ack-mode", "record"));
        ConfigurableApplicationContext gateway = start(GatewayApplication.class, true, Map.of(
                "spring.kafka.producer.acks", "all",
                "spring.kafka.producer.properties.enable.idempotence", "true",
                "gateway.send-timeout-ms", "6000"));
        gatewayPort = Integer.parseInt(gateway.getEnvironment().getProperty("local.server.port"));
    }

    @AfterAll
    static void stop() {
        // Reverse order: stop consumers/HTTP front doors before the things they depend on.
        for (int i = CONTEXTS.size() - 1; i >= 0; i--) {
            CONTEXTS.get(i).close();
        }
        REDIS.stop();
        POSTGRES.stop();
        KAFKA.stop();
    }

    @Test
    void purchaseAC2ConfirmsOrderWithinThirtySeconds() throws Exception {
        // Arrange
        String body = "{\"eventId\":\"" + EVENT_ID + "\",\"customerId\":\"cust-e2e\",\"quantity\":1}";

        // Act
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + gatewayPort + "/orders"))
                        .header("Content-Type", "application/json")
                        .header("Idempotency-Key", UUID.randomUUID().toString())
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());

        // Assert
        assertThat(response.statusCode()).isEqualTo(202);
        UUID orderId = UUID.fromString(EventJson.mapper().readTree(response.body()).get("orderId").asString());

        try (KafkaConsumer<String, Object> confirmedTopic = confirmedConsumer()) {
            List<ConsumerRecord<String, Object>> seen = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                confirmedTopic.poll(Duration.ofMillis(200)).forEach(seen::add);
                assertThat(forOrder(seen, orderId)).as("OrderConfirmed on orders.confirmed").isNotEmpty();
            });

            OrderConfirmed confirmed = (OrderConfirmed) forOrder(seen, orderId).get(0).value();
            assertThat(confirmed.orderId()).isEqualTo(orderId);
            assertThat(confirmed.eventId()).isEqualTo(EVENT_ID);
            assertThat(confirmed.customerId()).isEqualTo("cust-e2e");
            assertThat(confirmed.quantity()).isEqualTo(1);
            assertThat(confirmed.amountCents()).isEqualTo(5000L);
            assertThat(confirmed.currency()).isEqualTo("EUR");
            assertThat(forOrder(seen, orderId).get(0).key()).isEqualTo(orderId.toString());

            // Absence of a duplicate is only meaningful once order-service has consumed everything.
            awaitOrderServiceIdle();
            confirmedTopic.poll(Duration.ofMillis(500)).forEach(seen::add);
            assertThat(forOrder(seen, orderId)).as("exactly one OrderConfirmed").hasSize(1);
        }
        assertThat(orderStatus(orderId)).isEqualTo("CONFIRMED");
    }

    // ---- helpers ----

    private static ConfigurableApplicationContext start(Class<?> app, boolean web, Map<String, String> config) {
        List<String> args = new ArrayList<>(List.of(
                "--spring.config.location=optional:classpath:/e2e-no-shared-config/",
                "--spring.main.web-application-type=" + (web ? "servlet" : "none"),
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.kafka.consumer.auto-offset-reset=earliest",
                "--spring.autoconfigure.exclude=" + excludedAutoConfigurations(app)));
        config.forEach((k, v) -> args.add("--" + k + "=" + v));
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(app).run(args.toArray(String[]::new));
        CONTEXTS.add(ctx);
        return ctx;
    }

    /** Only order-service owns a database and only inventory-service uses Redis. */
    private static String excludedAutoConfigurations(Class<?> app) {
        List<String> excluded = new ArrayList<>();
        if (app != OrderServiceApplication.class) {
            excluded.add("org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration");
            excluded.add("org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration");
        }
        if (app != InventoryApplication.class) {
            excluded.add("org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration");
            excluded.add("org.springframework.boot.data.redis.autoconfigure.DataRedisRepositoriesAutoConfiguration");
        }
        return String.join(",", excluded.isEmpty() ? List.of("none.Placeholder") : excluded);
    }

    private static void createTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS_REQUESTED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.TICKETS_RESERVED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.ORDERS_REJECTED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.PAYMENTS_RESULT, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.ORDERS_CONFIRMED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.TICKETS_RELEASED, Topics.PARTITIONS, (short) 1))).all().get();
        }
    }

    private static KafkaConsumer<String, Object> confirmedConsumer() {
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "e2e-collector-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), EventJson.deserializer());
        consumer.subscribe(List.of(Topics.ORDERS_CONFIRMED));
        return consumer;
    }

    private static List<ConsumerRecord<String, Object>> forOrder(
            List<ConsumerRecord<String, Object>> records, UUID orderId) {
        return records.stream()
                .filter(r -> r.value() instanceof OrderConfirmed c && orderId.equals(c.orderId()))
                .toList();
    }

    /** order-service has committed every offset of its input topics (no sleep). */
    private static void awaitOrderServiceIdle() {
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(200)).until(() -> {
                Map<TopicPartition, OffsetAndMetadata> committed =
                        admin.listConsumerGroupOffsets(ORDER_GROUP).partitionsToOffsetAndMetadata().get();
                for (String topic : List.of(Topics.ORDERS_REQUESTED, Topics.PAYMENTS_RESULT, Topics.ORDERS_REJECTED)) {
                    for (int p = 0; p < Topics.PARTITIONS; p++) {
                        TopicPartition tp = new TopicPartition(topic, p);
                        long end = admin.listOffsets(Map.of(tp, OffsetSpec.latest()))
                                .partitionResult(tp).get().offset();
                        OffsetAndMetadata c = committed.get(tp);
                        if (end > 0 && (c == null || c.offset() < end)) {
                            return false;
                        }
                    }
                }
                return true;
            });
        }
    }

    private static String orderStatus(UUID orderId) throws Exception {
        try (Connection c = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             PreparedStatement ps = c.prepareStatement("select status from orders.orders where order_id = ?")) {
            ps.setObject(1, orderId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
}
