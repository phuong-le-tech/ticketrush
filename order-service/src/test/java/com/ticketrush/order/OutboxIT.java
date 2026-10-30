package com.ticketrush.order;

import static com.ticketrush.order.M4OrderTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.Status;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** No mocked DB, Kafka producer, acknowledgment, or transport. */
@SpringBootTest
@org.junit.jupiter.api.Tag("m4-red")
class OutboxIT {
    @Autowired OrderRepository repository;
    @Autowired JdbcClient jdbc;
    @Autowired KafkaTemplate<String, Object> kafka;
    @Autowired PlatformTransactionManager transactionManager;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        OrderTestInfra.registerProperties(registry);
        registry.add("spring.kafka.producer.acks", () -> "all");
        registry.add("spring.kafka.producer.properties.delivery.timeout.ms", () -> "3000");
        registry.add("spring.kafka.producer.properties.request.timeout.ms", () -> "1000");
        registry.add("spring.kafka.producer.properties.max.block.ms", () -> "1000");
    }

    @ParameterizedTest(name = "AC4 rollback and recovery for {0}")
    @CsvSource({
        "SUCCEEDED,FAILED,CONFIRMED,orders.confirmed,OrderConfirmed,ORDER_ID",
        "FAILED,SUCCEEDED,FAILED,tickets.released,TicketsReleased,m4-concert"
    })
    void AC4_forcedInsertFailureRollsBackStatusAndOutboxForBothEventTypes(
            Status result, Status opposite, String terminal, String topic, String type, String key) {
        requireOutbox(jdbc);
        UUID id = UUID.randomUUID();
        seed(repository, jdbc, id, "PENDING");
        var before = OrderTestInfra.rows(id);
        // Fail the actual outbox insert, after the status update, to expose missing transactions.
        jdbc.sql("""
                CREATE OR REPLACE FUNCTION orders.m4_reject_outbox_insert() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN
                IF NEW.payload ->> 'orderId' = '%s' THEN
                    RAISE EXCEPTION 'M4 forced outbox insert failure';
                END IF;
                RETURN NEW;
                END $$
                """.formatted(id)).update();
        jdbc.sql("CREATE TRIGGER m4_reject_outbox_insert BEFORE INSERT ON orders.outbox"
                + " FOR EACH ROW EXECUTE FUNCTION orders.m4_reject_outbox_insert()").update();
        try {
            assertThatThrownBy(() -> repository.onPayment(payment(id, result)))
                    .hasStackTraceContaining("M4 forced outbox insert failure");
            assertThat(OrderTestInfra.rows(id)).isEqualTo(before);
            assertThat(outbox(jdbc, id)).isEmpty();
        } finally {
            jdbc.sql("DROP TRIGGER m4_reject_outbox_insert ON orders.outbox").update();
            jdbc.sql("DROP FUNCTION orders.m4_reject_outbox_insert()").update();
        }
        var event = payment(id, result);
        repository.onPayment(event);
        assertThat(OrderTestInfra.statusOf(id)).isEqualTo(terminal);
        var rows = outbox(jdbc, id);
        assertThat(rows).hasSize(1);
        assertEvent(rows.getFirst(), id, topic, type, key.replace("ORDER_ID", id.toString()));
        repository.onPayment(event);
        repository.onPayment(payment(id, result));
        repository.onPayment(payment(id, opposite));
        assertThat(OrderTestInfra.statusOf(id)).isEqualTo(terminal);
        assertThat(outbox(jdbc, id)).isEqualTo(rows);
    }

    @Test
    void AC4_brokerOutageLeavesUnpublishedRecoveryPublishesCorrectKeyTypePayload() throws Exception {
        requireOutbox(jdbc);
        ensureReleaseTopic();
        UUID confirmed = UUID.randomUUID();
        UUID released = UUID.randomUUID();
        seed(repository, jdbc, confirmed, "PENDING");
        seed(repository, jdbc, released, "PENDING");
        // Warm metadata while the real broker is available; the outage must hit a real send.
        kafka.partitionsFor(Topics.ORDERS_CONFIRMED);
        kafka.partitionsFor(Topics.TICKETS_RELEASED);
        OutboxRelay relay = new OutboxRelay(jdbc, kafka, new TransactionTemplate(transactionManager));
        var docker = OrderTestInfra.KAFKA.getDockerClient();
        List<M4OrderTestSupport.OutboxRow> durable;
        docker.pauseContainerCmd(OrderTestInfra.KAFKA.getContainerId()).exec();
        try {
            // DB transitions must not depend on Kafka availability.
            repository.onPayment(payment(confirmed, Status.SUCCEEDED));
            repository.onPayment(payment(released, Status.FAILED));
            durable = new ArrayList<>(outbox(jdbc, confirmed));
            durable.addAll(outbox(jdbc, released));
            assertThat(durable).hasSize(2).allSatisfy(row -> assertThat(row.publishedAt()).isNull());
            assertEvent(durable.get(0), confirmed, Topics.ORDERS_CONFIRMED,
                    "OrderConfirmed", confirmed.toString());
            assertEvent(durable.get(1), released, Topics.TICKETS_RELEASED, "TicketsReleased", EVENT);
            // Either propagate a transport timeout or leave the batch pending for the next tick.
            assertThat(publishDuringOutage(relay)).satisfiesAnyOf(
                    failure -> assertThat(failure).isNull(),
                    failure -> assertThat(failure).hasStackTraceContaining("Timeout"));
            assertThat(outbox(jdbc, confirmed)).containsExactly(durable.get(0));
            assertThat(outbox(jdbc, released)).containsExactly(durable.get(1));
        } finally {
            docker.unpauseContainerCmd(OrderTestInfra.KAFKA.getContainerId()).exec();
        }
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, OrderTestInfra.KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "m4-outbox-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> partitions = relayPartitions();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            relay.publishPending();
            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                collectOrderRecords(consumer, received, List.of(confirmed.toString(), released.toString()));
                assertThat(received).anySatisfy(record -> assertPublished(record, durable.get(0)));
                assertThat(received).anySatisfy(record -> assertPublished(record, durable.get(1)));
                assertThat(outbox(jdbc, confirmed).getFirst().publishedAt()).isNotNull();
                assertThat(outbox(jdbc, released).getFirst().publishedAt()).isNotNull();
            });
            var afterConfirmation = outbox(jdbc, confirmed);
            var afterRelease = outbox(jdbc, released);
            var endBeforeReplay = consumer.endOffsets(partitions);
            relay.publishPending();
            relay.publishPending();
            assertThat(consumer.endOffsets(partitions)).isEqualTo(endBeforeReplay);
            assertThat(outbox(jdbc, confirmed)).isEqualTo(afterConfirmation);
            assertThat(outbox(jdbc, released)).isEqualTo(afterRelease);
        }
    }

    private static void assertPublished(ConsumerRecord<String, String> record,
                                        M4OrderTestSupport.OutboxRow row) {
        assertThat(record.topic()).isEqualTo(row.topic());
        assertThat(record.key()).isEqualTo(row.key());
        assertThat(record.headers().lastHeader("__TypeId__")).isNotNull();
        assertThat(new String(record.headers().lastHeader("__TypeId__").value(), StandardCharsets.UTF_8))
                .isEqualTo(row.type());
        assertThat(EventJson.mapper().readTree(record.value()))
                .isEqualTo(EventJson.mapper().readTree(row.payload()));
    }

    private static void assertEvent(M4OrderTestSupport.OutboxRow row, UUID id,
                                    String topic, String type, String key) {
        assertThat(row.id()).isNotNull();
        assertThat(row.createdAt()).isNotNull();
        assertThat(row.topic()).isEqualTo(topic);
        assertThat(row.key()).isEqualTo(key);
        assertThat(row.type()).isEqualTo(type);
        var body = EventJson.mapper().readTree(row.payload());
        assertThat(body.path("messageId").asString()).isNotBlank();
        assertThat(body.path("occurredAt").asString()).isNotBlank();
        var expected = new java.util.HashMap<String, Object>(Map.<String, Map<String, Object>>of(
                "OrderConfirmed", Map.of("customerId", "m4-customer", "amountCents", 12_550L, "currency", "EUR"),
                "TicketsReleased", Map.of("reason", "PAYMENT_FAILED")).get(type));
        expected.put("orderId", id.toString());
        expected.put("eventId", EVENT);
        expected.put("quantity", 3);
        expected.put("messageId", body.path("messageId").asString());
        expected.put("occurredAt", body.path("occurredAt").asString());
        assertThat(body).isEqualTo(EventJson.mapper().readTree(
                EventJson.mapper().writeValueAsString(expected)));
    }

    private static void ensureReleaseTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.<String, Object>of(
                "bootstrap.servers", OrderTestInfra.KAFKA.getBootstrapServers()))) {
            if (!admin.listTopics().names().get().contains(Topics.TICKETS_RELEASED)) {
                admin.createTopics(List.of(new NewTopic(Topics.TICKETS_RELEASED,
                        Topics.PARTITIONS, (short) 1))).all().get();
            }
        }
    }

    private static RuntimeException publishDuringOutage(OutboxRelay relay) {
        try {
            relay.publishPending();
            return null;
        } catch (RuntimeException failure) {
            return failure;
        }
    }

    private static List<TopicPartition> relayPartitions() {
        List<TopicPartition> partitions = new ArrayList<>();
        for (String topic : List.of(Topics.ORDERS_CONFIRMED, Topics.TICKETS_RELEASED)) {
            for (int p = 0; p < Topics.PARTITIONS; p++) {
                partitions.add(new TopicPartition(topic, p));
            }
        }
        return partitions;
    }

    private static void collectOrderRecords(KafkaConsumer<String, String> consumer,
                                            List<ConsumerRecord<String, String>> received,
                                            List<String> orderIds) {
        consumer.poll(Duration.ofMillis(200)).forEach(record -> {
            String orderId = EventJson.mapper().readTree(record.value()).path("orderId").asString();
            if (orderIds.contains(orderId)) {
                received.add(record);
            }
        });
    }
}
