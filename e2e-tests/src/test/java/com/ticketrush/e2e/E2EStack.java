package com.ticketrush.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import tools.jackson.databind.JsonNode;

/**
 * Milestone 2 shared fixture: real Kafka, Postgres, Redis and every service running in this JVM
 * (same approach as PurchaseFlowE2ETest). Not named *E2E, so failsafe does not run it as a test.
 */
final class E2EStack implements AutoCloseable {

    static final String PRICE_CURRENCY = "EUR";

    /** Consumer group -> input topics, used to decide when the whole saga has drained. */
    private static final Map<String, List<String>> GROUPS = Map.of(
            "order-service", List.of(Topics.ORDERS_REQUESTED, Topics.PAYMENTS_RESULT, Topics.ORDERS_REJECTED),
            "inventory-service", List.of(Topics.ORDERS_REQUESTED),
            "payment-service", List.of(Topics.TICKETS_RESERVED));
    private static final List<String> ALL_TOPICS = List.of(Topics.ORDERS_REQUESTED, Topics.TICKETS_RESERVED,
            Topics.ORDERS_REJECTED, Topics.PAYMENTS_RESULT, Topics.ORDERS_CONFIRMED, Topics.TICKETS_RELEASED);

    /** One HTTP attempt outcome. */
    record Attempt(int status, UUID orderId) {
    }

    /** Result of a client that retries 503 / connection errors with the same key until it gets 202. */
    record Accepted(UUID orderId, int retries) {
    }

    private final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");
    private final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18.6")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(Path.of("..", "deploy", "compose", "init-schemas.sql")
                            .toAbsolutePath().normalize()),
                    "/docker-entrypoint-initdb.d/init-schemas.sql");
    private final GenericContainer<?> redis = new GenericContainer<>("redis:8.10.2").withExposedPorts(6379);
    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();
    private final HttpClient http = HttpClient.newHttpClient();

    private int gatewayPort;
    private String providerUrl;
    private StringRedisTemplate redisTemplate;

    /** Boots everything. seedEventId / seedStock configure the inventory seed at startup. */
    static E2EStack start(String seedEventId, int seedStock) throws Exception {
        E2EStack stack = new E2EStack();
        stack.boot(seedEventId, seedStock);
        return stack;
    }

    private void boot(String seedEventId, int seedStock) throws Exception {
        kafka.start();
        postgres.start();
        redis.start();
        createTopics();

        ConfigurableApplicationContext provider = run(ProviderApplication.class, true, Map.of());
        providerUrl = "http://localhost:" + provider.getEnvironment().getProperty("local.server.port");

        run(OrderServiceApplication.class, false, Map.of(
                "spring.datasource.url", postgres.getJdbcUrl(),
                "spring.datasource.username", postgres.getUsername(),
                "spring.datasource.password", postgres.getPassword(),
                "spring.flyway.schemas", "orders",
                "spring.flyway.default-schema", "orders",
                "spring.kafka.consumer.group-id", "order-service",
                "spring.kafka.consumer.enable-auto-commit", "false",
                "spring.kafka.listener.ack-mode", "manual",
                "spring.kafka.listener.concurrency", "4"));
        ConfigurableApplicationContext inventory = run(InventoryApplication.class, false, Map.ofEntries(
                Map.entry("spring.data.redis.host", redis.getHost()),
                Map.entry("spring.data.redis.port", String.valueOf(redis.getMappedPort(6379))),
                Map.entry("spring.kafka.consumer.group-id", "inventory-service"),
                Map.entry("spring.kafka.consumer.enable-auto-commit", "false"),
                Map.entry("spring.kafka.listener.ack-mode", "manual"),
                Map.entry("spring.kafka.listener.concurrency", "4"),
                Map.entry("inventory.seed.event-id", seedEventId),
                Map.entry("inventory.seed.stock", String.valueOf(seedStock)),
                Map.entry("inventory.seed.price-cents", "5000"),
                Map.entry("inventory.seed.currency", PRICE_CURRENCY),
                Map.entry("inventory.reservation-ttl-seconds", "300")));
        redisTemplate = inventory.getBean(StringRedisTemplate.class);
        run(PaymentApplication.class, true, Map.of(
                "spring.kafka.consumer.group-id", "payment-service",
                "spring.kafka.consumer.enable-auto-commit", "false",
                "spring.kafka.listener.ack-mode", "manual",
                "spring.kafka.listener.concurrency", "4",
                "payment.provider.url", providerUrl,
                "payment.provider.timeout-ms", "5000"));
        run(NotificationApplication.class, false, Map.of(
                "spring.kafka.consumer.group-id", "notification-service",
                "spring.kafka.listener.ack-mode", "record"));
        ConfigurableApplicationContext gateway = run(GatewayApplication.class, true, Map.of(
                "spring.kafka.producer.acks", "all",
                "spring.kafka.producer.properties.enable.idempotence", "true",
                "gateway.send-timeout-ms", "6000"));
        gatewayPort = Integer.parseInt(gateway.getEnvironment().getProperty("local.server.port"));
    }

    @Override
    public void close() {
        // Reverse order: stop HTTP front doors and consumers before what they depend on.
        for (int i = contexts.size() - 1; i >= 0; i--) {
            contexts.get(i).close();
        }
        redis.stop();
        postgres.stop();
        kafka.stop();
    }

    // ---- HTTP ----

    /** One POST /orders attempt. status -1 means a connection level failure (counts as retryable). */
    Attempt post(String idempotencyKey, String customerId, String eventId, int quantity) {
        String body = "{\"eventId\":\"" + eventId + "\",\"customerId\":\"" + customerId
                + "\",\"quantity\":" + quantity + "}";
        try {
            HttpResponse<String> response = http.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + gatewayPort + "/orders"))
                            .timeout(Duration.ofSeconds(60))
                            .header("Content-Type", "application/json")
                            .header("Idempotency-Key", idempotencyKey)
                            .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            UUID orderId = response.statusCode() == 202
                    ? UUID.fromString(EventJson.mapper().readTree(response.body()).get("orderId").asString())
                    : null;
            return new Attempt(response.statusCode(), orderId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (java.io.IOException e) {
            return new Attempt(-1, null);
        }
    }

    /**
     * Behaves like a well-behaved client: a 503 or a dropped connection is retried with the SAME
     * key until 202. Any other status (400, 500...) is a failure. Small pause between retries is
     * client backoff, not a wait for a server condition.
     */
    Accepted postUntilAccepted(String idempotencyKey, String customerId, String eventId, int quantity,
                               Duration giveUpAfter) throws InterruptedException {
        long deadline = System.nanoTime() + giveUpAfter.toNanos();
        int retries = 0;
        while (true) {
            Attempt attempt = post(idempotencyKey, customerId, eventId, quantity);
            if (attempt.status() == 202) {
                return new Accepted(attempt.orderId(), retries);
            }
            boolean retryable = attempt.status() == 503 || attempt.status() == -1;
            assertThat(retryable).as("POST /orders status %s for key %s", attempt.status(), idempotencyKey).isTrue();
            assertThat(System.nanoTime()).as("gave up retrying key " + idempotencyKey).isLessThan(deadline);
            retries++;
            try {
                Thread.sleep(50);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted during retry backoff", ie);
            }
        }
    }

    // ---- Redis ----

    StringRedisTemplate redis() {
        return redisTemplate;
    }

    void seedStock(String eventId, long stock) {
        String key = "inventory:event:" + eventId;
        redisTemplate.delete(key);
        redisTemplate.opsForHash().put(key, "stock", String.valueOf(stock));
        redisTemplate.opsForHash().put(key, "priceCents", "5000");
        redisTemplate.opsForHash().put(key, "currency", PRICE_CURRENCY);
    }

    String stock(String eventId) {
        return (String) redisTemplate.opsForHash().get("inventory:event:" + eventId, "stock");
    }

    // ---- Postgres ----

    Map<String, Long> statusCounts(String eventId) throws Exception {
        Map<String, Long> counts = new HashMap<>();
        try (Connection c = connection();
             PreparedStatement ps = c.prepareStatement(
                     "select status, count(*) from orders.orders where event_id = ? group by status")) {
            ps.setString(1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    counts.put(rs.getString(1), rs.getLong(2));
                }
            }
        }
        return counts;
    }

    long totalOrderRows() throws Exception {
        try (Connection c = connection();
             PreparedStatement ps = c.prepareStatement("select count(*) from orders.orders");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    String orderStatus(UUID orderId) throws Exception {
        try (Connection c = connection();
             PreparedStatement ps = c.prepareStatement("select status from orders.orders where order_id = ?")) {
            ps.setObject(1, orderId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
    }

    // ---- Provider ----

    /** Number of charges the fake provider holds for this order (its idempotency key is the orderId). */
    int chargeCount(UUID orderId) throws Exception {
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create(providerUrl + "/charges?idempotencyKey=" + orderId)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return EventJson.mapper().readTree(response.body()).size();
    }

    // ---- Kafka ----

    String bootstrapServers() {
        return kafka.getBootstrapServers();
    }

    /** Every record currently on the topic (key + raw value), read from the beginning, no sleeps. */
    List<ConsumerRecord<String, byte[]>> readAll(String topic) {
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "e2e-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"),
                new StringDeserializer(), new ByteArrayDeserializer())) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (int p = 0; p < Topics.PARTITIONS; p++) {
                partitions.add(new TopicPartition(topic, p));
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            List<ConsumerRecord<String, byte[]>> out = new ArrayList<>();
            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(10)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(out::add);
                return partitions.stream().allMatch(tp -> consumer.position(tp) >= end.get(tp));
            });
            return out;
        }
    }

    /** The orderId carried in a record value (every event has one). */
    static UUID orderIdOf(ConsumerRecord<String, byte[]> msg) {
        JsonNode node = EventJson.mapper().readTree(msg.value());
        return UUID.fromString(node.get("orderId").asString());
    }

    /**
     * The whole saga has drained: every service committed every offset of its input topics, and no
     * record was produced anywhere during the last four samples (a replayed message may cascade
     * through several topics, so lag 0 on one sample is not enough).
     */
    void awaitQuiescent(Duration atMost) {
        AtomicLong lastTotal = new AtomicLong(-1);
        AtomicInteger stableSamples = new AtomicInteger();
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            await().atMost(atMost).pollInterval(Duration.ofMillis(500)).until(() -> {
                long total = totalEndOffsets(admin);
                boolean drained = lagIsZero(admin);
                if (drained && total == lastTotal.get()) {
                    stableSamples.incrementAndGet();
                } else {
                    stableSamples.set(0);
                }
                lastTotal.set(total);
                return stableSamples.get() >= 4;
            });
        }
    }

    /** True once the three consumer groups have no lag (committed offsets equal end offsets). */
    boolean lagIsZero(AdminClient admin) throws Exception {
        for (Map.Entry<String, List<String>> group : GROUPS.entrySet()) {
            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(group.getKey()).partitionsToOffsetAndMetadata().get();
            for (String topic : group.getValue()) {
                for (int p = 0; p < Topics.PARTITIONS; p++) {
                    TopicPartition tp = new TopicPartition(topic, p);
                    long end = admin.listOffsets(Map.of(tp, OffsetSpec.latest())).partitionResult(tp).get().offset();
                    OffsetAndMetadata c = committed.get(tp);
                    if (end > 0 && (c == null || c.offset() < end)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private long totalEndOffsets(AdminClient admin) throws Exception {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (String topic : ALL_TOPICS) {
            for (int p = 0; p < Topics.PARTITIONS; p++) {
                request.put(new TopicPartition(topic, p), OffsetSpec.latest());
            }
        }
        long total = 0;
        for (var info : admin.listOffsets(request).all().get().values()) {
            total += info.offset();
        }
        return total;
    }

    // ---- startup plumbing ----

    private ConfigurableApplicationContext run(Class<?> app, boolean web, Map<String, String> config) {
        List<String> args = new ArrayList<>(List.of(
                "--spring.config.location=optional:classpath:/e2e-no-shared-config/",
                "--spring.main.web-application-type=" + (web ? "servlet" : "none"),
                "--server.port=0",
                "--spring.jmx.enabled=false",
                "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                "--spring.kafka.consumer.auto-offset-reset=earliest",
                "--spring.autoconfigure.exclude=" + excludedAutoConfigurations(app)));
        config.forEach((k, v) -> args.add("--" + k + "=" + v));
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(app).run(args.toArray(String[]::new));
        contexts.add(ctx);
        return ctx;
    }

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

    private void createTopics() throws Exception {
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()))) {
            List<NewTopic> topics = new ArrayList<>();
            for (String topic : ALL_TOPICS) {
                topics.add(new NewTopic(topic, Topics.PARTITIONS, (short) 1));
            }
            admin.createTopics(topics).all().get();
        }
    }

    /** Distinct values helper kept here so tests stay free of loops. */
    static <T> Set<T> distinct(List<T> values) {
        return new HashSet<>(values);
    }
}
