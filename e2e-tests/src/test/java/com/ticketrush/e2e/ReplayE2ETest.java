package com.ticketrush.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.Topics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Milestone 2 acceptance (replay and retry): re-publishing byte-identical Kafka records, and
 * re-POSTing with the same Idempotency-Key, never changes stock, charges, statuses or the
 * OrderConfirmed count. Each test seeds its own event so the two do not interfere.
 */
class ReplayE2ETest {

    private static final Duration SETTLE = Duration.ofSeconds(60);

    private static E2EStack stack;

    @BeforeAll
    static void startStack() throws Exception {
        stack = E2EStack.start("seed-event", 1);
    }

    @AfterAll
    static void stopStack() {
        stack.close();
    }

    /** Snapshot of everything that must stay unchanged by a replay. */
    private record Observed(String stock, int chargesA, int chargesB, String statusA, String statusB,
                            long confirmedCount, long rejectedForA) {
    }

    @Test
    void replayAC3ChangesNothing() throws Exception {
        // Arrange: stock 1, order A takes it and is confirmed, order B finds it sold out.
        String eventId = "replay-event";
        stack.seedStock(eventId, 1);
        UUID orderA = stack.postUntilAccepted("replay-key-a", "replay-customer-a", eventId, 1, SETTLE).orderId();
        awaitStatus(orderA, "CONFIRMED");
        UUID orderB = stack.postUntilAccepted("replay-key-b", "replay-customer-b", eventId, 1, SETTLE).orderId();
        awaitStatus(orderB, "REJECTED");
        stack.awaitQuiescent(SETTLE);
        Observed before = observe(orderA, orderB, eventId);
        List<ConsumerRecord<String, byte[]>> captured = new ArrayList<>();
        captured.addAll(recordsOf(Topics.ORDERS_REQUESTED, orderA, orderB));
        captured.addAll(recordsOf(Topics.TICKETS_RESERVED, orderA));
        captured.addAll(recordsOf(Topics.PAYMENTS_RESULT, orderA));
        assertThat(captured).as("captured OrderRequested x2, TicketsReserved, PaymentResult").hasSize(4);

        // Act: 3 byte-identical replays (same key, same value bytes, same headers incl. type header).
        for (int round = 0; round < 3; round++) {
            republish(captured);
        }
        stack.awaitQuiescent(SETTLE);

        // Assert: nothing observable changed.
        Observed after = observe(orderA, orderB, eventId);
        assertThat(before).isEqualTo(new Observed("0", 1, 0, "CONFIRMED", "REJECTED", 1, 0));
        assertThat(after).isEqualTo(before);
    }

    @Test
    void repostSameKeyAC4AfterConfirmation() throws Exception {
        // Arrange
        String eventId = "repost-event";
        stack.seedStock(eventId, 1);
        UUID orderA = stack.postUntilAccepted("repost-key", "repost-customer", eventId, 1, SETTLE).orderId();
        awaitStatus(orderA, "CONFIRMED");
        stack.awaitQuiescent(SETTLE);
        Observed before = observe(orderA, null, eventId);

        // Act
        E2EStack.Attempt again = stack.post("repost-key", "repost-customer", eventId, 1);
        stack.awaitQuiescent(SETTLE);

        // Assert
        assertThat(again.status()).isEqualTo(202);
        assertThat(again.orderId()).isEqualTo(orderA);
        assertThat(observe(orderA, null, eventId)).isEqualTo(before);
        assertThat(before.stock()).isEqualTo("0");
        assertThat(before.chargesA()).isEqualTo(1);
        assertThat(before.confirmedCount()).isEqualTo(1);
    }

    // ---- helpers ----

    private static void awaitStatus(UUID orderId, String expected) {
        await().atMost(SETTLE).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(stack.orderStatus(orderId)).isEqualTo(expected));
    }

    private static Observed observe(UUID orderA, UUID orderB, String eventId) throws Exception {
        long confirmed = stack.readAll(Topics.ORDERS_CONFIRMED).stream()
                .filter(r -> EventJson.mapper().readValue(r.value(), OrderConfirmed.class).eventId().equals(eventId))
                .count();
        long rejectedForA = stack.readAll(Topics.ORDERS_REJECTED).stream()
                .filter(r -> E2EStack.orderIdOf(r).equals(orderA))
                .count();
        return new Observed(stack.stock(eventId), stack.chargeCount(orderA),
                orderB == null ? 0 : stack.chargeCount(orderB),
                stack.orderStatus(orderA), orderB == null ? null : stack.orderStatus(orderB),
                confirmed, rejectedForA);
    }

    private static List<ConsumerRecord<String, byte[]>> recordsOf(String topic, UUID... orderIds) {
        List<UUID> wanted = List.of(orderIds);
        return stack.readAll(topic).stream().filter(r -> wanted.contains(E2EStack.orderIdOf(r))).toList();
    }

    private static void republish(List<ConsumerRecord<String, byte[]>> records) throws Exception {
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, stack.bootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all"),
                new StringSerializer(), new ByteArraySerializer())) {
            for (ConsumerRecord<String, byte[]> original : records) {
                producer.send(new ProducerRecord<>(original.topic(), null, original.key(), original.value(),
                        original.headers())).get();
            }
        }
    }
}
