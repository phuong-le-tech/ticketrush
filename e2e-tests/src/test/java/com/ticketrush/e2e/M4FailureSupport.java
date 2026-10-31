package com.ticketrush.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.ticketrush.common.EventJson;
import com.ticketrush.common.Topics;
import com.ticketrush.payment.PaymentApplication;
import com.ticketrush.payment.ProviderClient;
import com.ticketrush.provider.ProviderApplication;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.util.ReflectionTestUtils;

/** Test plumbing only. All saga decisions, charges and inventory writes stay in real services. */
final class M4FailureSupport {
    static final Duration SETTLE = Duration.ofSeconds(30);
    static final Duration AC7_DEADLINE = Duration.ofSeconds(120);
    static final Instant START = Instant.parse("2099-01-01T00:00:00Z");

    private M4FailureSupport() {
    }

    static E2EStack start(String event, int stock, String failureRate) throws Exception {
        return E2EStack.start(event, stock, Map.of(
                "FAKE_PROVIDER_FAILURE_RATE", failureRate,
                "fake-provider.failure-rate", failureRate));
    }

    static UUID buy(E2EStack stack, String event, String buyer) throws InterruptedException {
        return stack.postUntilAccepted("key-" + buyer, buyer, event, 1, SETTLE).orderId();
    }

    static void awaitStatus(E2EStack stack, UUID id, String status) {
        await().atMost(SETTLE).untilAsserted(() -> assertThat(stack.orderStatus(id)).isEqualTo(status));
    }

    static void awaitStock(E2EStack stack, String event, int expected) {
        await().atMost(SETTLE).untilAsserted(() -> assertThat(stack.stock(event)).isEqualTo("" + expected));
    }

    static List<ConsumerRecord<String, byte[]>> records(E2EStack stack, String topic, UUID id) {
        return stack.readAll(topic).stream().filter(r -> E2EStack.orderIdOf(r).equals(id)).toList();
    }

    static <T> List<T> events(E2EStack stack, String topic, Class<T> type) {
        return stack.readAll(topic).stream().map(r -> EventJson.mapper().readValue(r.value(), type)).toList();
    }

    static void requireOutbox(E2EStack stack) throws Exception {
        try (var connection = stack.connection();
             var query = connection.prepareStatement("select to_regclass('orders.outbox')");
             var rows = query.executeQuery()) {
            rows.next();
            assertThat(rows.getString(1))
                    .as("M4 must migrate orders.outbox before querying transactional deliveries").isNotNull();
        }
    }

    static long outboxCount(E2EStack stack, UUID id, String topic) throws Exception {
        requireOutbox(stack);
        try (var connection = stack.connection();
             var query = connection.prepareStatement(
                     "select count(*) from orders.outbox where payload->>'orderId' = ? and topic = ?")) {
            query.setString(1, id.toString());
            query.setString(2, topic);
            try (var rows = query.executeQuery()) {
                rows.next();
                return rows.getLong(1);
            }
        }
    }

    static void awaitOutboxPublished(E2EStack stack) throws Exception {
        requireOutbox(stack);
        await().atMost(SETTLE).untilAsserted(() -> {
            try (var connection = stack.connection();
                 var query = connection.prepareStatement(
                         "select count(*) from orders.outbox where published_at is null");
                 var rows = query.executeQuery()) {
                rows.next();
                assertThat(rows.getLong(1)).as("unpublished outbox rows").isZero();
            }
        });
    }

    static void awaitReleaseConsumed(E2EStack stack) {
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(
                Map.of("bootstrap.servers", stack.bootstrapServers()))) {
            var partitions = IntStream.range(0, Topics.PARTITIONS)
                    .mapToObj(p -> new org.apache.kafka.common.TopicPartition(Topics.TICKETS_RELEASED, p)).toList();
            Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> request =
                    partitions.stream().collect(java.util.stream.Collectors.toMap(p -> p,
                            p -> org.apache.kafka.clients.admin.OffsetSpec.latest()));
            await().atMost(SETTLE).untilAsserted(() -> {
                var ends = admin.listOffsets(request).all().get(10, TimeUnit.SECONDS);
                var committed = admin.listConsumerGroupOffsets("inventory-service")
                        .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                var unread = partitions.stream().filter(p -> ends.get(p).offset() > 0)
                        .filter(p -> !committed.containsKey(p)
                                || committed.get(p).offset() < ends.get(p).offset()).toList();
                assertThat(unread).as("inventory release records not yet committed").isEmpty();
            });
        }
    }

    static void stopPayment(E2EStack stack) {
        var registry = stack.context(PaymentApplication.class).getBean(KafkaListenerEndpointRegistry.class);
        registry.stop();
        await().atMost(SETTLE).untilAsserted(() ->
                assertThat(registry.getListenerContainers()).allSatisfy(c -> assertThat(c.isRunning()).isFalse()));
    }

    static void setSagaAgeAndStatus(E2EStack stack, UUID id, String status) throws Exception {
        try (var connection = stack.connection();
             PreparedStatement update = connection.prepareStatement("""
                     update orders.orders set status = ?, created_at = ?, updated_at = ? where order_id = ?
                     """)) {
            update.setString(1, status);
            update.setTimestamp(2, Timestamp.from(START));
            update.setTimestamp(3, Timestamp.from(START));
            update.setObject(4, id);
            assertThat(update.executeUpdate()).isEqualTo(1);
        }
    }

    static void replayConcurrently(E2EStack stack, List<ConsumerRecord<String, byte[]>> records, int copies)
            throws Exception {
        CyclicBarrier ready = new CyclicBarrier(copies);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = IntStream.range(0, copies).mapToObj(i -> workers.submit(() -> {
                ready.await(30, TimeUnit.SECONDS);
                try (var producer = new KafkaProducer<String, byte[]>(
                        Map.of("bootstrap.servers", stack.bootstrapServers(), "acks", "all"),
                        new StringSerializer(), new ByteArraySerializer())) {
                    for (var original : records) {
                        producer.send(new ProducerRecord<>(original.topic(), null, original.key(), original.value(),
                                original.headers())).get(30, TimeUnit.SECONDS);
                    }
                }
                return null;
            })).toList();
            for (var future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        }
    }

    static List<UUID> buyConcurrently(E2EStack stack, String event, int count) throws Exception {
        try (var clients = Executors.newVirtualThreadPerTaskExecutor()) {
            var requests = IntStream.range(0, count)
                    .mapToObj(i -> clients.submit(() -> buy(stack, event, "buyer-" + i))).toList();
            List<UUID> ids = new ArrayList<>();
            for (var request : requests) {
                ids.add(request.get(120, TimeUnit.SECONDS));
            }
            return ids;
        }
    }

    static List<Integer> chargeCounts(E2EStack stack, List<UUID> ids) throws Exception {
        List<Integer> counts = new ArrayList<>();
        for (UUID id : ids) {
            counts.add(stack.chargeCount(id));
        }
        return counts;
    }

    enum Fault { HTTP500, LOST_REPLY }

    /** One real HTTP fault, then transparent forwarding to the real fake-provider application. */
    static final class FaultProxy implements AutoCloseable {
        private final HttpServer server;
        private final HttpClient http = HttpClient.newHttpClient();
        private final URI upstream;
        private final Fault fault;
        private final AtomicInteger attempts = new AtomicInteger();
        private final List<String> keys = new CopyOnWriteArrayList<>();
        private final List<String> bodies = new CopyOnWriteArrayList<>();
        private final List<Throwable> errors = new CopyOnWriteArrayList<>();
        private final List<Integer> upstreamStatuses = new CopyOnWriteArrayList<>();
        private final java.util.concurrent.ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        FaultProxy(E2EStack stack, Fault fault) throws IOException {
            this.fault = fault;
            String port = stack.context(ProviderApplication.class).getEnvironment().getProperty("local.server.port");
            upstream = URI.create("http://localhost:" + port + "/charges");
            server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            server.setExecutor(executor);
            server.createContext("/charges", this::handle);
            server.start();
            // ProviderClient captures its URI at construction. Change only its HTTP destination,
            // not its behavior or Kafka listener, so retries still exercise production code.
            ReflectionTestUtils.setField(stack.context(PaymentApplication.class).getBean(ProviderClient.class),
                    "chargesUri", URI.create("http://localhost:" + server.getAddress().getPort() + "/charges"));
        }

        private void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                String key = exchange.getRequestHeaders().getFirst("Idempotency-Key");
                String body = new String(exchange.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                keys.add(key);
                bodies.add(body);
                int attempt = attempts.incrementAndGet();
                if (attempt == 1 && fault == Fault.HTTP500) {
                    exchange.sendResponseHeaders(500, -1);
                    return;
                }
                var response = http.send(HttpRequest.newBuilder(upstream)
                                .header("Content-Type", "application/json").header("Idempotency-Key", key)
                                .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                upstreamStatuses.add(response.statusCode());
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                if (attempt == 1 && fault == Fault.LOST_REPLY) {
                    // Charge has committed upstream. An incomplete reply forces a client read failure.
                    exchange.sendResponseHeaders(200, response.body().length + 100L);
                    exchange.getResponseBody().write('{');
                    return;
                }
                exchange.sendResponseHeaders(response.statusCode(), response.body().length);
                exchange.getResponseBody().write(response.body());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                errors.add(e);
            } catch (IOException | RuntimeException e) {
                errors.add(e);
            }
        }

        List<String> keys() { return List.copyOf(keys); }
        List<String> bodies() { return List.copyOf(bodies); }
        List<Throwable> errors() { return List.copyOf(errors); }
        List<Integer> upstreamStatuses() { return List.copyOf(upstreamStatuses); }

        @Override
        public void close() {
            server.stop(0);
            executor.close();
        }
    }
}
