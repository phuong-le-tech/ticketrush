package com.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.ReservationRejected;
import com.ticketrush.common.Status;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Uses real listener/repository transactions with the shared Kafka/Postgres fixtures. */
@Tag("m5-red")
@SpringBootTest(properties = {"server.port=0", "spring.kafka.listener.auto-startup=false",
        "order.saga.reaper-enabled=false"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MetricsTest {
    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry properties) {
        OrderTestInfra.registerProperties(properties);
    }

    @Autowired MeterRegistry registry;
    @Autowired OrderSagaListener listener;

    @ParameterizedTest
    @ValueSource(strings = {"orders.requested", "payments.result", "orders.rejected"})
    void AC2_timerRegisteredAtZero(String topic) {
        check(timer(topic), 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders.requested", "payments.result", "orders.rejected"})
    void AC2_timerCountsOneAttempt(String topic) {
        Timer timer = timer(topic);
        check(timer, 0);
        attempt(topic);
        check(timer, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders.requested", "payments.result", "orders.rejected"})
    void AC2_timerCountsConcurrentAttempts(String topic) throws Exception {
        Timer timer = timer(topic);
        check(timer, 0);
        CountDownLatch start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(8)) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                tasks.add(pool.submit(() -> {
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    attempt(topic);
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> task : tasks) task.get(30, TimeUnit.SECONDS);
        }
        check(timer, 8);
    }

    private void attempt(String topic) {
        UUID id = UUID.randomUUID();
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        var acknowledgments = new java.util.concurrent.atomic.AtomicInteger();
        switch (topic) {
            case "orders.requested" -> listener.onRequested(new OrderRequested(UUID.randomUUID(), id,
                    id.toString(), "event-" + id, "customer-" + id, 1, now), acknowledgments::incrementAndGet);
            case "payments.result" -> listener.onPayment(new PaymentResult(UUID.randomUUID(), id,
                    "event-" + id, Status.FAILED, 5000L, "EUR", null, "PROVIDER_DECLINED", now),
                    acknowledgments::incrementAndGet);
            case "orders.rejected" -> listener.onRejected(new ReservationRejected(UUID.randomUUID(), id,
                    "event-" + id, "SOLD_OUT", now), acknowledgments::incrementAndGet);
            default -> throw new IllegalArgumentException(topic);
        }
        assertThat(acknowledgments.get()).isEqualTo(1);
    }

    private Timer timer(String topic) {
        Timer timer = registry.find("ticketrush.consumer.processing")
                .tags("consumer", "order-service", "topic", topic).timer();
        assertThat(timer).as("missing application processing timer for order-service/%s", topic).isNotNull();
        return timer;
    }

    private void check(Timer timer, long count) {
        assertThat(timer.count()).isEqualTo(count).isNotNegative();
        assertThat(timer.totalTime(TimeUnit.SECONDS)).isFinite().isNotNegative();
        for (Timer registered : registry.find("ticketrush.consumer.processing").timers()) {
            assertThat(registered.getId().getTags()).extracting(tag -> tag.getKey())
                    .containsExactlyInAnyOrder("consumer", "topic");
            assertThat(registered.getId().getTag("consumer")).isEqualTo("order-service");
            assertThat(registered.getId().getTag("topic"))
                    .isIn("orders.requested", "payments.result", "orders.rejected");
        }
        assertThat(registry.find("ticketrush.consumer.processing").timers()).hasSize(3);
    }
}
