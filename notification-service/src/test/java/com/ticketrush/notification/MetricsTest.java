package com.ticketrush.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.ticketrush.common.OrderConfirmed;
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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

/** Real Spring listener bean; no test-owned timer registration or recording. */
@Tag("m5-red")
@SpringBootTest(properties = {"server.port=0", "spring.kafka.listener.auto-startup=false"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MetricsTest {
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");
    static { KAFKA.start(); }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry properties) {
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired MeterRegistry registry;
    @Autowired OrderConfirmedListener listener;

    @Test
    void AC2_timerRegisteredAtZero() {
        check(timer(), 0);
    }

    @Test
    void AC2_timerCountsOneAttempt() {
        Timer timer = timer();
        check(timer, 0);
        attempt();
        check(timer, 1);
    }

    @Test
    void AC2_timerCountsConcurrentAttempts() throws Exception {
        Timer timer = timer();
        check(timer, 0);
        CountDownLatch start = new CountDownLatch(1);
        try (var pool = Executors.newFixedThreadPool(8)) {
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                tasks.add(pool.submit(() -> {
                    assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    attempt();
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> task : tasks) task.get(30, TimeUnit.SECONDS);
        }
        check(timer, 8);
    }

    private void attempt() {
        UUID id = UUID.randomUUID();
        listener.onOrderConfirmed(new OrderConfirmed(UUID.randomUUID(), id, "event-" + id,
                "customer-" + id, 1, 5000L, "EUR", Instant.parse("2026-01-01T00:00:00Z")));
    }

    private Timer timer() {
        Timer timer = registry.find("ticketrush.consumer.processing")
                .tags("consumer", "notification-service", "topic", "orders.confirmed").timer();
        assertThat(timer).as("missing application processing timer for notification-service/orders.confirmed").isNotNull();
        return timer;
    }

    private void check(Timer timer, long count) {
        assertThat(timer.count()).isEqualTo(count).isNotNegative();
        assertThat(timer.totalTime(TimeUnit.SECONDS)).isFinite().isNotNegative();
        assertThat(timer.getId().getTags()).extracting(tag -> tag.getKey())
                .containsExactlyInAnyOrder("consumer", "topic");
        assertThat(timer.getId().getTag("consumer")).isEqualTo("notification-service");
        assertThat(timer.getId().getTag("topic")).isEqualTo("orders.confirmed");
        assertThat(registry.find("ticketrush.consumer.processing").timers()).hasSize(1);
    }
}
