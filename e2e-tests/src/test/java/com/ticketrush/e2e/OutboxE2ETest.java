package com.ticketrush.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.Topics;
import com.ticketrush.order.OrderServiceApplication;
import com.ticketrush.order.OutboxRelay;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Real broker acknowledgments and a Postgres trigger reproduce the publish/mark crash window. */
@ExtendWith(OutputCaptureExtension.class)
@org.junit.jupiter.api.Tag("m4-red")
class OutboxE2ETest {
    private static final Duration DEADLINE = Duration.ofSeconds(30);

    @Test
    void AC9_realContainersAndNotificationOutputCapture(CapturedOutput output) throws Exception {
        try (E2EStack stack = E2EStack.start("ac9-real-capture", 1, Map.of())) {
            UUID orderId = M4FailureSupport.buy(stack, "ac9-real-capture", "capture-buyer");
            M4FailureSupport.awaitStatus(stack, orderId, "CONFIRMED");
            await().alias("OutputCapture sees the actual notification listener")
                    .atMost(DEADLINE).untilAsserted(() ->
                            assertThat(output.getAll().lines().filter(line ->
                                    line.contains("NOTIFY orderId=" + orderId + " customerId=")).count())
                                    .isEqualTo(1L));
            assertThat(stack.stock("ac9-real-capture")).isEqualTo("0");
            assertThat(stack.totalOrderRows()).isEqualTo(1);
            assertThat(stack.chargeCount(orderId)).isEqualTo(1);
            assertThat(M4FailureSupport.records(stack, Topics.ORDERS_CONFIRMED, orderId)).hasSize(1);
        }
    }

    @Test
    void AC4_concurrentRelaysAndCrashAfterAckReplayYieldOneNotification(CapturedOutput output) throws Exception {
        // Only the two relays below may publish: the scheduled one would re-send the trigger-blocked row every tick.
        try (E2EStack stack = E2EStack.start("outbox-seed", 2, Map.of(), Map.of("order.outbox.relay-enabled", "false"))) {
            var context = stack.context(OrderServiceApplication.class);
            JdbcClient jdbc = context.getBean(JdbcClient.class);
            assertThat(jdbc.sql("select count(*) from information_schema.tables "
                    + "where table_schema = 'orders' and table_name = 'outbox'").query(Long.class).single())
                    .as("M4 must migrate the transactional outbox before relays can deliver").isEqualTo(1L);
            AtomicReference<OutboxRelay> first = new AtomicReference<>();
            AtomicReference<OutboxRelay> second = new AtomicReference<>();
            var transactions = new TransactionTemplate(context.getBean(PlatformTransactionManager.class));
            assertThatCode(() -> {
                first.set(new OutboxRelay(jdbc, template(context.getBean(KafkaTemplate.class)), transactions));
                second.set(new OutboxRelay(jdbc, template(context.getBean(KafkaTemplate.class)), transactions));
            }).as("production outbox relays must be executable, not compile-only contracts")
                    .doesNotThrowAnyException();

            OrderConfirmed oldest = confirmed("outbox-crash");
            OrderConfirmed unlockedA = confirmed("outbox-unlocked-a");
            OrderConfirmed unlockedB = confirmed("outbox-unlocked-b");
            UUID oldestId = enqueue(jdbc, oldest, Instant.now().minusSeconds(30));
            UUID unlockedAId = enqueue(jdbc, unlockedA, Instant.now().minusSeconds(20));
            UUID unlockedBId = enqueue(jdbc, unlockedB, Instant.now().minusSeconds(10));

            // Hold the oldest row on a separate real connection. SKIP LOCKED must let both
            // relay workers finish without waiting for it or publishing that locked record.
            try (Connection lock = stack.connection()) {
                lock.setAutoCommit(false);
                try (var statement = lock.prepareStatement("select id from orders.outbox where id = ? for update")) {
                    statement.setObject(1, oldestId);
                    statement.executeQuery().close();
                }
                try {
                    publishConcurrently(first.get(), second.get());
                    assertThat(published(jdbc, unlockedAId)).isTrue();
                    assertThat(published(jdbc, unlockedBId)).isTrue();
                    assertThat(published(jdbc, oldestId)).isFalse();
                    assertThat(records(stack, oldest)).isEmpty();
                    assertThat(records(stack, unlockedA)).hasSize(1);
                    assertThat(records(stack, unlockedB)).hasSize(1);
                    await().alias("both unlocked notifications emitted by the real listener")
                            .atMost(DEADLINE).untilAsserted(() -> {
                                assertThat(notifications(output, unlockedA)).isEqualTo(1L);
                                assertThat(notifications(output, unlockedB)).isEqualTo(1L);
                            });
                } finally {
                    lock.rollback();
                }
            }

            // Test-only fault: Kafka's actual send must ack, then the DB mark fails. No mock
            // producer or fabricated acknowledgment can satisfy the broker record assertion.
            jdbc.sql("""
                    create function orders.m4_fail_outbox_mark() returns trigger language plpgsql as $$
                    begin
                      if NEW.id = '%s'::uuid and NEW.published_at is not null then
                        raise exception 'test crash after Kafka ack before outbox mark';
                      end if;
                      return NEW;
                    end $$
                    """.formatted(oldestId)).update();
            jdbc.sql("create trigger m4_fail_outbox_mark before update on orders.outbox "
                    + "for each row execute function orders.m4_fail_outbox_mark()").update();
            Throwable markFailure;
            try {
                markFailure = catchThrowable(first.get()::publishPending);
                await().alias("real Kafka ack precedes failed outbox mark").atMost(DEADLINE)
                        .untilAsserted(() -> assertThat(records(stack, oldest)).hasSize(1));
                assertThat(markFailure).as("forced post-ack DB failure must surface to relay caller").isNotNull();
                assertThat(published(jdbc, oldestId)).as("failed mark leaves replayable outbox row").isFalse();
                await().atMost(DEADLINE).untilAsserted(() ->
                        assertThat(notifications(output, oldest)).isEqualTo(1L));
            } finally {
                jdbc.sql("drop trigger m4_fail_outbox_mark on orders.outbox").update();
                jdbc.sql("drop function orders.m4_fail_outbox_mark()").update();
            }

            publishConcurrently(first.get(), second.get());
            await().atMost(DEADLINE).untilAsserted(() -> assertThat(published(jdbc, oldestId)).isTrue());
            assertThat(records(stack, oldest)).as("same persisted event republished after the crash").hasSize(2);
            ConsumerRecord<String, byte[]> replay = records(stack, oldest).get(1);
            TopicPartition replayPartition = new TopicPartition(replay.topic(), replay.partition());
            try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", stack.bootstrapServers()))) {
                await().alias("notification group committed the replay before counting side effects")
                        .atMost(DEADLINE).untilAsserted(() -> {
                            var committed = admin.listConsumerGroupOffsets("notification-service")
                                    .partitionsToOffsetAndMetadata().get();
                            assertThat(committed).containsKey(replayPartition);
                            assertThat(committed.get(replayPartition).offset()).isGreaterThan(replay.offset());
                        });
            }
            assertThat(records(stack, oldest)).extracting(record ->
                    EventJson.mapper().readValue(record.value(), OrderConfirmed.class).messageId())
                    .containsExactly(oldest.messageId(), oldest.messageId());
            assertThat(records(stack, oldest)).extracting(ConsumerRecord::key)
                    .containsExactly(oldest.orderId().toString(), oldest.orderId().toString());
            assertThat(notifications(output, oldest)).as("one real NOTIFY side effect despite two deliveries")
                    .isEqualTo(1L);
            assertThat(notifications(output, unlockedA)).isEqualTo(1L);
            assertThat(notifications(output, unlockedB)).isEqualTo(1L);
            assertThat(records(stack, unlockedA)).hasSize(1);
            assertThat(records(stack, unlockedB)).hasSize(1);
            assertThat(stack.stock("outbox-seed")).as("real Redis stock remains untouched by notification replay")
                    .isEqualTo("2");
        }
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, Object> template(KafkaTemplate<?, ?> template) {
        return (KafkaTemplate<String, Object>) template;
    }

    private static OrderConfirmed confirmed(String eventId) {
        return new OrderConfirmed(UUID.randomUUID(), UUID.randomUUID(), eventId,
                "outbox-customer", 1, 5000L, "EUR", Instant.now());
    }

    private static UUID enqueue(JdbcClient jdbc, OrderConfirmed event, Instant createdAt) {
        UUID id = UUID.randomUUID();
        jdbc.sql("""
                insert into orders.outbox (id, topic, msg_key, type, payload, created_at, published_at)
                values (:id, :topic, :key, :type, cast(:payload as jsonb), :created, null)
                """).param("id", id).param("topic", Topics.ORDERS_CONFIRMED)
                .param("key", event.orderId().toString()).param("type", "OrderConfirmed")
                .param("payload", EventJson.mapper().writeValueAsString(event))
                .param("created", java.sql.Timestamp.from(createdAt)).update();
        return id;
    }

    private static boolean published(JdbcClient jdbc, UUID id) {
        return jdbc.sql("select published_at is not null from orders.outbox where id = :id")
                .param("id", id).query(Boolean.class).single();
    }

    private static List<ConsumerRecord<String, byte[]>> records(E2EStack stack, OrderConfirmed event) {
        return stack.readAll(Topics.ORDERS_CONFIRMED).stream()
                .filter(record -> event.orderId().toString().equals(record.key())).toList();
    }

    private static long notifications(CapturedOutput output, OrderConfirmed event) {
        return output.getAll().lines().filter(line -> line.contains("NOTIFY orderId=" + event.orderId()
                + " customerId=")).count();
    }

    private static void publishConcurrently(OutboxRelay first, OutboxRelay second) throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            var a = executor.submit(() -> {
                ready.countDown();
                start.await();
                first.publishPending();
                return null;
            });
            var b = executor.submit(() -> {
                ready.countDown();
                start.await();
                second.publishPending();
                return null;
            });
            assertThat(ready.await(10, TimeUnit.SECONDS)).as("both real relay workers ready").isTrue();
            start.countDown();
            assertThatCode(() -> a.get(15, TimeUnit.SECONDS)).as("first concurrent relay must finish")
                    .doesNotThrowAnyException();
            assertThatCode(() -> b.get(15, TimeUnit.SECONDS)).as("second concurrent relay must finish")
                    .doesNotThrowAnyException();
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }
}
