package com.ticketrush.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.Topics;
import com.ticketrush.gateway.GatewayApplication;
import com.ticketrush.inventory.InventoryApplication;
import com.ticketrush.payment.PaymentApplication;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/** Real service traffic and broker offsets, without assuming an exporter-specific lag metric name. */
@Tag("m5-red")
class MetricsE2ETest {
    @Test
    void AC2_gatewayCounterMatchesAcknowledgmentsIncludingRetries() throws Exception {
        try (E2EStack stack = E2EStack.start("m5-ack", 4)) {
            Counter orders = orders(stack);
            double before = orders.count();

            var first = stack.post("m5-key", "m5-customer", "m5-ack", 1);
            var retry = stack.post("m5-key", "m5-customer", "m5-ack", 1);

            assertThat(first.status()).isEqualTo(202);
            assertThat(retry.status()).isEqualTo(202);
            assertThat(retry.orderId()).isEqualTo(first.orderId());
            assertThat(orders.count() - before).isEqualTo(2);
            assertThat(orders.getId().getTags()).isEmpty();
            stack.awaitQuiescent(Duration.ofSeconds(60));
            assertThat(stack.totalOrderRows()).isEqualTo(1);
            assertThat(stack.chargeCount(first.orderId())).isEqualTo(1);
        }
    }

    @Test
    void AC2_lagRisesThenDrainsAndProcessingCountsRise() throws Exception {
        try (E2EStack stack = E2EStack.start("m5-lag", 4);
             AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", stack.bootstrapServers()))) {
            Timer inventory = processing(stack, InventoryApplication.class, "inventory-service", Topics.ORDERS_REQUESTED);
            Timer payment = processing(stack, PaymentApplication.class, "payment-service", Topics.TICKETS_RESERVED);
            long inventoryBefore = inventory.count();
            long paymentBefore = payment.count();
            var listeners = stack.context(InventoryApplication.class).getBean(KafkaListenerEndpointRegistry.class);
            listeners.stop();
            await().atMost(Duration.ofSeconds(20)).until(() -> !listeners.isRunning());

            var accepted = stack.post("m5-backlog-key", "m5-backlog-customer", "m5-lag", 1);

            assertThat(accepted.status()).isEqualTo(202);
            assertThat(lag(admin, "inventory-service", Topics.ORDERS_REQUESTED)).isEqualTo(1);
            assertThat(inventory.count()).isEqualTo(inventoryBefore);
            listeners.start();
            stack.awaitQuiescent(Duration.ofSeconds(60));
            assertThat(lag(admin, "inventory-service", Topics.ORDERS_REQUESTED)).isZero();
            assertThat(lag(admin, "payment-service", Topics.TICKETS_RESERVED)).isZero();
            assertThat(inventory.count() - inventoryBefore).isEqualTo(1);
            assertThat(payment.count() - paymentBefore).isEqualTo(1);
            assertThat(inventory.totalTime(TimeUnit.SECONDS)).isFinite().isNotNegative();
            assertThat(payment.totalTime(TimeUnit.SECONDS)).isFinite().isNotNegative();
            assertThat(stack.orderStatus(accepted.orderId())).isEqualTo("CONFIRMED");
        }
    }

    @Test
    void AC2_httpScrapesAfterTrafficWithoutSensitiveLabels() throws Exception {
        try (E2EStack stack = E2EStack.startMetrics("m5-http", 4)) {
            var accepted = stack.post("m5-http-key", "m5-http-customer", "m5-http", 1);
            assertThat(accepted.status()).isEqualTo(202);
            stack.awaitQuiescent(Duration.ofSeconds(60));

            assertScrape(stack, GatewayApplication.class, "ticketrush_orders_total", accepted.orderId().toString());
            assertScrape(stack, InventoryApplication.class, "ticketrush_consumer_processing_seconds_count", accepted.orderId().toString());
            assertScrape(stack, PaymentApplication.class, "ticketrush_consumer_processing_seconds_count", accepted.orderId().toString());
            assertScrape(stack, com.ticketrush.order.OrderServiceApplication.class,
                    "ticketrush_saga_outcomes_total", accepted.orderId().toString());
            assertScrape(stack, com.ticketrush.notification.NotificationApplication.class,
                    "ticketrush_consumer_processing_seconds_count", accepted.orderId().toString());
            assertScrape(stack, com.ticketrush.provider.ProviderApplication.class,
                    "jvm_memory_used_bytes", accepted.orderId().toString());
        }
    }

    private void assertScrape(E2EStack stack, Class<?> app, String metric, String orderId) throws Exception {
        String port = stack.context(app).getEnvironment().getProperty("local.server.port");
        var request = java.net.http.HttpRequest.newBuilder(java.net.URI.create(
                "http://localhost:" + port + "/actuator/prometheus")).timeout(Duration.ofSeconds(10)).GET().build();
        var response = java.net.http.HttpClient.newHttpClient().send(request,
                java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s scrape after traffic", app.getSimpleName()).isEqualTo(200);
        assertThat(response.body()).containsPattern("(?m)^" + metric + "(?:\\{[^\\n]*\\})? [0-9].*$");
        assertThat(response.body()).doesNotContain(orderId, "m5-http-key", "m5-http-customer");
        assertThat(response.body()).doesNotContainPattern(
                "(?i)(?:\\{|,)\\s*(?:order_?id|customer_?id|event_?id|message_?id|idempotency_?key|password|secret|token|payload|exception_?message|provider_?charge_?id)=");
    }

    private Counter orders(E2EStack stack) {
        Counter counter = stack.context(GatewayApplication.class).getBean(MeterRegistry.class)
                .find("ticketrush.orders").counter();
        assertThat(counter).as("gateway must register the acknowledged-order counter before traffic").isNotNull();
        return counter;
    }

    private Timer processing(E2EStack stack, Class<?> app, String consumer, String topic) {
        Timer timer = stack.context(app).getBean(MeterRegistry.class)
                .find("ticketrush.consumer.processing").tags("consumer", consumer, "topic", topic).timer();
        assertThat(timer).as("missing application processing timer for %s/%s", consumer, topic).isNotNull();
        return timer;
    }

    private long lag(AdminClient admin, String group, String topic) throws Exception {
        var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        long lag = 0;
        for (int partition = 0; partition < Topics.PARTITIONS; partition++) {
            var tp = new TopicPartition(topic, partition);
            long end = admin.listOffsets(Map.of(tp, OffsetSpec.latest())).partitionResult(tp).get().offset();
            long offset = committed.containsKey(tp) ? committed.get(tp).offset() : 0;
            lag += Math.max(0, end - offset);
        }
        return lag;
    }
}
