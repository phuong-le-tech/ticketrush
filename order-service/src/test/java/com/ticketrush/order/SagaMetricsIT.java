package com.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.ReservationRejected;
import com.ticketrush.common.Status;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Real Spring transactions and infrastructure; only the reaper clock is fixed. */
@SpringBootTest
@Tag("m5-red")
class SagaMetricsIT {
    private static final String METRIC = "ticketrush.saga.outcomes";
    // Earlier than the existing fixtures: this reaper cannot expire another test's active rows.
    private static final Instant CREATED = Instant.parse("2000-01-01T00:00:00Z");
    private static final String EVENT = "m5-saga-metrics";
    private final UUID id = UUID.randomUUID();

    @Autowired OrderRepository repository;
    @Autowired MeterRegistry registry;
    @Autowired JdbcClient jdbc;
    @Autowired PlatformTransactionManager transactionManager;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry properties) {
        OrderTestInfra.registerProperties(properties);
        properties.add("order.outbox.relay-enabled", () -> "false");
    }

    @AfterEach
    void removeOnlyThisTestsRows() {
        jdbc.sql("DELETE FROM orders.outbox WHERE payload ->> 'orderId' = :id")
                .param("id", id.toString()).update();
        jdbc.sql("DELETE FROM orders.orders WHERE order_id = :id").param("id", id).update();
    }

    @ParameterizedTest(name = "AC2 payment {0} counts committed {1} exactly once")
    @CsvSource({"SUCCEEDED,CONFIRMED", "FAILED,FAILED"})
    void AC2_paymentTerminalTransitionsExactlyOnce(Status paymentStatus, String outcome) throws Exception {
        seed();
        var before = counts();
        PaymentResult payment = payment(paymentStatus);

        repository.onPayment(payment);

        assertThat(OrderTestInfra.statusOf(id)).isEqualTo(outcome);
        assertDelta(before, outcome, 1);
        var terminal = OrderTestInfra.rows(id);
        var committed = counts();
        repository.onPayment(payment);
        repository.onPayment(payment(paymentStatus));
        concurrentReplay(() -> repository.onPayment(payment));
        repository.onRequested(requested());
        assertThat(OrderTestInfra.rows(id)).isEqualTo(terminal);
        assertThat(counts()).isEqualTo(committed);
    }

    @Test
    void AC2_rejectionCountsOnceAndConcurrentReplayAddsZero() throws Exception {
        seed();
        var before = counts();
        ReservationRejected rejection = rejected();

        repository.onRejected(rejection);

        assertThat(OrderTestInfra.statusOf(id)).isEqualTo("REJECTED");
        assertDelta(before, "REJECTED", 1);
        var terminal = OrderTestInfra.rows(id);
        var committed = counts();
        repository.onRejected(rejection);
        repository.onRejected(rejected());
        concurrentReplay(() -> repository.onRejected(rejection));
        repository.onRequested(requested());
        assertThat(OrderTestInfra.rows(id)).isEqualTo(terminal);
        assertThat(counts()).isEqualTo(committed);
    }

    @Test
    void AC2_timeoutCountsOnceAndConcurrentReplayAddsZero() throws Exception {
        seed();
        var before = counts();
        SagaReaper reaper = reaper();

        reaper.reapExpired();

        assertThat(OrderTestInfra.statusOf(id)).isEqualTo("FAILED");
        assertThat(OrderTestInfra.rows(id).getFirst().failureReason()).isEqualTo("TIMEOUT");
        assertDelta(before, "FAILED", 1);
        var terminal = OrderTestInfra.rows(id);
        var committed = counts();
        reaper.reapExpired();
        concurrentReplay(reaper::reapExpired);
        repository.onPayment(payment(Status.SUCCEEDED));
        repository.onPayment(payment(Status.FAILED));
        repository.onRequested(requested());
        assertThat(OrderTestInfra.rows(id)).isEqualTo(terminal);
        assertThat(counts()).isEqualTo(committed);
    }

    @ParameterizedTest(name = "AC2 rolled-back {0} adds no outcome")
    @ValueSource(strings = {"CONFIRMED", "FAILED", "REJECTED", "TIMEOUT"})
    void AC2_rollbackLeavesStateAndEveryCounterUnchanged(String transition) {
        seed();
        var initial = OrderTestInfra.rows(id);
        var initialOutbox = M4OrderTestSupport.outbox(jdbc, id);
        var before = counts();

        new TransactionTemplate(transactionManager).executeWithoutResult(transaction -> {
            switch (transition) {
                case "CONFIRMED" -> repository.onPayment(payment(Status.SUCCEEDED));
                case "FAILED" -> repository.onPayment(payment(Status.FAILED));
                case "REJECTED" -> repository.onRejected(rejected());
                case "TIMEOUT" -> reaper().reapExpired();
                default -> throw new AssertionError("Unknown transition: " + transition);
            }
            assertThat(repository.statusOf(id)).contains(
                    "TIMEOUT".equals(transition) ? "FAILED" : transition);
            assertThat(counts()).as("uncommitted transitions must not be counted").isEqualTo(before);
            transaction.setRollbackOnly();
        });

        assertThat(OrderTestInfra.rows(id)).isEqualTo(initial);
        assertThat(M4OrderTestSupport.outbox(jdbc, id)).isEqualTo(initialOutbox);
        assertThat(counts()).as("rolled-back transitions must not be counted").isEqualTo(before);
    }

    private void seed() {
        repository.onRequested(requested());
        jdbc.sql("UPDATE orders.orders SET created_at = :at WHERE order_id = :id")
                .param("at", Timestamp.from(CREATED)).param("id", id).update();
        assertThat(OrderTestInfra.statusOf(id)).isEqualTo("PENDING");
    }

    private OrderRequested requested() {
        return new OrderRequested(UUID.randomUUID(), id, "m5-metrics-" + id, EVENT,
                "m5-metrics-customer", 1, CREATED);
    }

    private PaymentResult payment(Status status) {
        return new PaymentResult(UUID.randomUUID(), id, EVENT, status, 5_000L, "EUR",
                status == Status.SUCCEEDED ? "charge-" + id : null,
                status == Status.FAILED ? "CARD_DECLINED" : null, CREATED.plusSeconds(1));
    }

    private ReservationRejected rejected() {
        return new ReservationRejected(UUID.randomUUID(), id, EVENT, "SOLD_OUT", CREATED.plusSeconds(1));
    }

    private SagaReaper reaper() {
        return new SagaReaper(repository, Clock.fixed(CREATED.plusSeconds(121), ZoneOffset.UTC));
    }

    private Map<String, Double> counts() {
        Map<String, Double> counts = new LinkedHashMap<>();
        for (String outcome : new String[] {"CONFIRMED", "FAILED", "REJECTED"}) {
            var counter = registry.find(METRIC).tag("outcome", outcome).counter();
            assertThat(counter)
                    .as("M5 must register %s{outcome=%s} for committed saga transitions", METRIC, outcome)
                    .isNotNull();
            counts.put(outcome, counter.count());
        }
        return counts;
    }

    private void assertDelta(Map<String, Double> before, String outcome, double delta) {
        Map<String, Double> expected = new LinkedHashMap<>(before);
        expected.compute(outcome, (key, count) -> count + delta);
        assertThat(counts()).as("only the committed terminal outcome changes").isEqualTo(expected);
    }

    private void concurrentReplay(Runnable replay) throws Exception {
        int workers = 8;
        CyclicBarrier start = new CyclicBarrier(workers);
        try (var pool = Executors.newFixedThreadPool(workers)) {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < workers; i++) {
                tasks.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    replay.run();
                    return null;
                }));
            }
            for (var task : tasks) {
                task.get(20, TimeUnit.SECONDS);
            }
        }
    }
}
