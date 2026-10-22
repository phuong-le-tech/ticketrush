package com.ticketrush.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.Status;
import com.ticketrush.common.Topics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Milestone 2 acceptance (contention): 10,000 buyers, 500 tickets, exactly 500 sold. 1,000 of the
 * buyers also send a concurrent duplicate POST with the same Idempotency-Key. Real Kafka, Postgres
 * and Redis, every service in this JVM, nothing mocked.
 */
class OversellE2ETest {

    private static final String EVENT_ID = "concert-1";
    private static final int STOCK = 500;
    private static final int BUYERS = 10_000;
    private static final int DUPLICATED_BUYERS = 1_000;
    private static final Duration SETTLE_BUDGET = Duration.ofSeconds(120);

    private static E2EStack stack;

    @BeforeAll
    static void startStack() throws Exception {
        stack = E2EStack.start(EVENT_ID, STOCK);
    }

    @AfterAll
    static void stopStack() {
        stack.close();
    }

    @Test
    void oversellAC1AC2Sells500Of10000() throws Exception {
        // Arrange
        long startedAt = System.nanoTime();
        Map<Integer, Future<E2EStack.Accepted>> first = new HashMap<>();
        Map<Integer, Future<E2EStack.Accepted>> duplicate = new HashMap<>();

        // Act: everybody fires at once (virtual threads), duplicates race their original.
        try (ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor()) {
            IntStream.range(0, BUYERS).forEach(i -> {
                first.put(i, clients.submit(() -> buy(i)));
                if (i < DUPLICATED_BUYERS) {
                    duplicate.put(i, clients.submit(() -> buy(i)));
                }
            });
        }

        // Assert AC1: every buyer got 202 (buy() retries 503 with the same key), duplicates share the orderId.
        Map<Integer, UUID> orderIds = new HashMap<>();
        for (Map.Entry<Integer, Future<E2EStack.Accepted>> e : first.entrySet()) {
            orderIds.put(e.getKey(), e.getValue().get().orderId());
        }
        assertThat(orderIds.values()).as("one distinct orderId per unique key").doesNotHaveDuplicates();
        assertThat(duplicatesThatDiffer(duplicate, orderIds)).as("duplicate POSTs with a different orderId").isEmpty();

        // Assert AC2: within 120 s of the first request the saga has settled on 500 / 9,500 / 0.
        Duration remaining = SETTLE_BUDGET.minus(Duration.ofNanos(System.nanoTime() - startedAt));
        assertThat(remaining).as("time left of the 120 s budget after sending").isPositive();
        assertThat(stack.statusCounts(EVENT_ID)).isNotEmpty(); // guard before containsOnly
        await().atMost(remaining).pollInterval(Duration.ofSeconds(1)).untilAsserted(() ->
                assertThat(stack.statusCounts(EVENT_ID)).containsOnly(
                        Map.entry("CONFIRMED", (long) STOCK), Map.entry("REJECTED", (long) (BUYERS - STOCK))));
        assertThat(stack.totalOrderRows()).isEqualTo(BUYERS);
        assertThat(stack.stock(EVENT_ID)).isEqualTo("0");

        // Assert: exactly 500 OrderConfirmed, distinct orderIds, once everything has drained.
        stack.awaitQuiescent(Duration.ofSeconds(60));
        List<UUID> confirmed = stack.readAll(Topics.ORDERS_CONFIRMED).stream()
                .map(r -> EventJson.mapper().readValue(r.value(), OrderConfirmed.class).orderId())
                .toList();
        assertThat(confirmed).hasSize(STOCK);
        assertThat(E2EStack.distinct(confirmed)).hasSize(STOCK);

        // Assert: each confirmed order was charged exactly once.
        Map<UUID, Set<String>> chargeIdsByOrder = stack.readAll(Topics.PAYMENTS_RESULT).stream()
                .map(r -> EventJson.mapper().readValue(r.value(), PaymentResult.class))
                .filter(p -> p.status() == Status.SUCCEEDED)
                .collect(Collectors.groupingBy(PaymentResult::orderId,
                        Collectors.mapping(PaymentResult::providerChargeId, Collectors.toSet())));
        assertThat(chargeIdsByOrder.keySet()).containsExactlyInAnyOrderElementsOf(confirmed);
        assertThat(chargeIdsByOrder.values()).isNotEmpty().allSatisfy(ids -> assertThat(ids).hasSize(1));
        assertThat(chargeIdsByOrder.values().stream().flatMap(Set::stream).distinct().count())
                .as("distinct provider chargeIds").isEqualTo(STOCK);
        assertThat(providerChargeCounts(confirmed)).isNotEmpty().allSatisfy(count -> assertThat(count).isEqualTo(1));
    }

    private static E2EStack.Accepted buy(int i) throws InterruptedException {
        return stack.postUntilAccepted("oversell-key-" + i, "oversell-customer-" + i, EVENT_ID, 1,
                Duration.ofSeconds(100));
    }

    private static List<String> duplicatesThatDiffer(Map<Integer, Future<E2EStack.Accepted>> duplicate,
                                                     Map<Integer, UUID> orderIds) throws Exception {
        List<String> mismatches = new ArrayList<>();
        for (Map.Entry<Integer, Future<E2EStack.Accepted>> e : duplicate.entrySet()) {
            UUID duplicateId = e.getValue().get().orderId();
            if (!duplicateId.equals(orderIds.get(e.getKey()))) {
                mismatches.add("buyer " + e.getKey() + ": " + orderIds.get(e.getKey()) + " vs " + duplicateId);
            }
        }
        return mismatches;
    }

    private static List<Integer> providerChargeCounts(List<UUID> orderIds) throws Exception {
        List<Integer> counts = new ArrayList<>();
        for (UUID orderId : orderIds) {
            counts.add(stack.chargeCount(orderId));
        }
        return counts;
    }
}
