package com.ticketrush.order;

import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.Topics;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Infrastructure partagee des tests order-service: Postgres 18.6 et Kafka 4.3.1 reels
 * (Testcontainers), demarres une fois par JVM. Aucun mock.
 *
 * Contrat de configuration vise (code-writer):
 * - spring.datasource.* et spring.kafka.bootstrap-servers (config Boot standard)
 * - Flyway sur le schema "orders" (spring.flyway.schemas=orders), migration creant orders.orders
 * - groupe de consommation "order-service" sur orders.requested, payments.result, orders.rejected
 * - commit d'offset apres traitement (awaitIdle s'appuie sur les offsets commit du groupe)
 */
final class OrderTestInfra {

    static final String GROUP_ID = "order-service";
    static final List<String> INPUT_TOPICS = List.of(
            Topics.ORDERS_REQUESTED, Topics.PAYMENTS_RESULT, Topics.ORDERS_REJECTED);

    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(
                            Path.of("..", "deploy", "compose", "init-schemas.sql").toAbsolutePath().normalize()),
                    "/docker-entrypoint-initdb.d/init-schemas.sql");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    private static final List<ConsumerRecord<String, Object>> CONFIRMED = new CopyOnWriteArrayList<>();
    private static final Map<TopicPartition, Long> COLLECTED_NEXT_OFFSET = new ConcurrentHashMap<>();

    static {
        POSTGRES.start();
        KAFKA.start();
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS_REQUESTED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.PAYMENTS_RESULT, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.ORDERS_REJECTED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.ORDERS_CONFIRMED, Topics.PARTITIONS, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        startConfirmedCollector();
    }

    private OrderTestInfra() {
    }

    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.flyway.schemas", () -> "orders");
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
    }

    // ---- Kafka ----

    /** Envoie un evenement, cle = orderId, et attend l'ack du broker. */
    static void send(String topic, UUID orderId, Object event) {
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                new StringSerializer(), EventJson.serializer())) {
            producer.send(new ProducerRecord<>(topic, orderId.toString(), event)).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Les OrderConfirmed vus sur orders.confirmed pour cette commande (cle = orderId). */
    static List<ConsumerRecord<String, Object>> confirmedFor(UUID orderId) {
        List<ConsumerRecord<String, Object>> out = new ArrayList<>();
        for (ConsumerRecord<String, Object> r : CONFIRMED) {
            if (orderId.toString().equals(r.key())
                    || (r.value() instanceof OrderConfirmed c && orderId.equals(c.orderId()))) {
                out.add(r);
            }
        }
        return out;
    }

    /**
     * Attend que le groupe order-service ait commit tous les offsets des topics d'entree.
     * Permet de prouver une absence (aucun OrderConfirmed) sans Thread.sleep.
     * Un groupe sans offset commit sur une partition non vide n'est pas "idle".
     */
    static void awaitIdle() {
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200)).until(() -> {
                Map<TopicPartition, OffsetAndMetadata> committed =
                        admin.listConsumerGroupOffsets(GROUP_ID).partitionsToOffsetAndMetadata().get();
                for (String topic : INPUT_TOPICS) {
                    for (int p = 0; p < Topics.PARTITIONS; p++) {
                        TopicPartition tp = new TopicPartition(topic, p);
                        long end = admin.listOffsets(Map.of(tp, OffsetSpec.latest()))
                                .partitionResult(tp).get().offset();
                        if (end == 0) {
                            continue;
                        }
                        OffsetAndMetadata c = committed.get(tp);
                        if (c == null || c.offset() < end) {
                            return false;
                        }
                    }
                }
                // Le collecteur de orders.confirmed doit aussi avoir rattrape le broker.
                for (int p = 0; p < Topics.PARTITIONS; p++) {
                    TopicPartition tp = new TopicPartition(Topics.ORDERS_CONFIRMED, p);
                    long end = admin.listOffsets(Map.of(tp, OffsetSpec.latest()))
                            .partitionResult(tp).get().offset();
                    if (COLLECTED_NEXT_OFFSET.getOrDefault(tp, 0L) < end) {
                        return false;
                    }
                }
                return true;
            });
        }
    }

    private static void startConfirmedCollector() {
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-collector-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), EventJson.deserializer());
        consumer.subscribe(List.of(Topics.ORDERS_CONFIRMED));
        AtomicBoolean running = new AtomicBoolean(true);
        Thread t = new Thread(() -> {
            try {
                while (running.get()) {
                    consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                        CONFIRMED.add(r);
                        COLLECTED_NEXT_OFFSET.merge(
                                new TopicPartition(r.topic(), r.partition()), r.offset() + 1, Math::max);
                    });
                }
            } finally {
                consumer.close();
            }
        }, "orders-confirmed-collector");
        t.setDaemon(true);
        t.start();
    }

    // ---- Postgres (JDBC direct, independant du contexte Spring) ----

    static Connection connection() throws SQLException {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    /** Ligne orders.orders lue telle quelle (null pour une colonne SQL NULL). */
    record Row(UUID orderId, String idempotencyKey, String eventId, String customerId,
               Integer quantity, String status, Long amountCents, String currency,
               String failureReason) {
    }

    static List<Row> rows(UUID orderId) {
        String sql = "select order_id, idempotency_key, event_id, customer_id, quantity, status,"
                + " amount_cents, currency, failure_reason from orders.orders where order_id = ?";
        try (Connection c = connection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, orderId);
            List<Row> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Row(rs.getObject("order_id", UUID.class),
                            rs.getString("idempotency_key"), rs.getString("event_id"),
                            rs.getString("customer_id"), (Integer) rs.getObject("quantity"),
                            rs.getString("status"), (Long) rs.getObject("amount_cents"),
                            rs.getString("currency"), rs.getString("failure_reason")));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    static String statusOf(UUID orderId) {
        List<Row> rows = rows(orderId);
        return rows.isEmpty() ? null : rows.get(0).status();
    }
}
