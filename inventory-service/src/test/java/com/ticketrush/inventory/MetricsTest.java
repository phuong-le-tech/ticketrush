package com.ticketrush.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.TicketsReleased;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Real listener beans and stores; listeners are invoked directly to isolate attempt counts. */
@Tag("m5-red")
@SpringBootTest(properties = {"server.port=0", "spring.kafka.listener.auto-startup=false"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class MetricsTest {
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:8.10.2").withExposedPorts(6379);
    static { KAFKA.start(); REDIS.start(); }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry properties) {
        properties.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        properties.add("spring.data.redis.host", REDIS::getHost);
        properties.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired MeterRegistry registry;
    @Autowired OrderRequestedListener requested;
    @Autowired TicketsReleasedListener released;

    @ParameterizedTest
    @ValueSource(strings = {"orders.requested", "tickets.released"})
    void AC2_timerRegisteredAtZero(String topic) {
        check(timer(topic), 0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders.requested", "tickets.released"})
    void AC2_timerCountsOneAttempt(String topic) throws Exception {
        Timer timer = timer(topic);
        check(timer, 0);
        attempt(topic);
        check(timer, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orders.requested", "tickets.released"})
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

    private void attempt(String topic) throws Exception {
        UUID id = UUID.randomUUID();
        var acknowledgments = new java.util.concurrent.atomic.AtomicInteger();
        if (topic.equals("orders.requested")) {
            requested.onOrderRequested(new OrderRequested(UUID.randomUUID(), id, id.toString(),
                    "missing-" + id, "customer-" + id, 1, Instant.parse("2026-01-01T00:00:00Z")), acknowledgments::incrementAndGet);
        } else {
            released.onTicketsReleased(new TicketsReleased(UUID.randomUUID(), id, "missing-" + id,
                    1, "TIMEOUT", Instant.parse("2026-01-01T00:00:00Z")), acknowledgments::incrementAndGet);
        }
        assertThat(acknowledgments.get()).isEqualTo(1);
    }

    private Timer timer(String topic) {
        Timer timer = registry.find("ticketrush.consumer.processing")
                .tags("consumer", "inventory-service", "topic", topic).timer();
        assertThat(timer).as("missing application processing timer for inventory-service/%s", topic).isNotNull();
        return timer;
    }

    private void check(Timer timer, long count) {
        assertThat(timer.count()).isEqualTo(count).isNotNegative();
        assertThat(timer.totalTime(TimeUnit.SECONDS)).isFinite().isNotNegative();
        for (Timer registered : registry.find("ticketrush.consumer.processing").timers()) {
            assertThat(registered.getId().getTags()).extracting(tag -> tag.getKey())
                    .containsExactlyInAnyOrder("consumer", "topic");
            assertThat(registered.getId().getTag("consumer")).isEqualTo("inventory-service");
            assertThat(registered.getId().getTag("topic")).isIn("orders.requested", "tickets.released");
        }
        assertThat(registry.find("ticketrush.consumer.processing").timers()).hasSize(2);
    }
}
