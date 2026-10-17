package com.ticketrush.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.ticketrush.common.EventJson;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.Status;
import com.ticketrush.common.TicketsReserved;
import com.ticketrush.common.Topics;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;

/**
 * Kafka reel (Testcontainers) et vrai client HTTP; seul le provider est un stub (serveur JDK sur port
 * aleatoire, pour prouver un vrai timeout de socket). Contrat de configuration vise (code-writer):
 * - spring.kafka.bootstrap-servers (config Boot standard, env KAFKA_BOOTSTRAP_SERVERS)
 * - payment.provider.url        = base URL du provider, env PAYMENT_PROVIDER_URL (ex. http://localhost:8083).
 *                                 Le service appelle POST {url}/charges.
 * - payment.provider.timeout-ms = env PAYMENT_PROVIDER_TIMEOUT_MS, applique en connexion ET en lecture.
 * Contrat HTTP vers le provider: header Idempotency-Key = orderId (jamais une cle fabriquee),
 * body JSON {orderId, amountCents, currency}; reponse 200 {chargeId, status}.
 * Contrat Kafka: consomme tickets.reserved, publie PaymentResult sur payments.result (cle = orderId),
 * messageId neuf, offset valide seulement apres publication (non teste ici), pas de retry en M1.
 */
@SpringBootTest(properties = {
        "server.port=0",
        "payment.provider.timeout-ms=500",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
class PaymentFlowIT {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");
    private static final StubProvider PROVIDER = new StubProvider();

    static {
        KAFKA.start();
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.TICKETS_RESERVED, Topics.PARTITIONS, (short) 1),
                    new NewTopic(Topics.PAYMENTS_RESULT, Topics.PARTITIONS, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("payment.provider.url", PROVIDER::baseUrl);
    }

    @AfterAll
    static void stopStub() {
        PROVIDER.stop();
    }

    private KafkaConsumer<String, Object> resultConsumer;

    @BeforeEach
    void setUp() {
        PROVIDER.reset();
        resultConsumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-results-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), EventJson.deserializer());
        resultConsumer.subscribe(List.of(Topics.PAYMENTS_RESULT));
    }

    @AfterEach
    void tearDown() {
        PROVIDER.release();
        resultConsumer.close();
    }

    private static TicketsReserved reserved(UUID orderId, long amountCents) {
        Instant now = Instant.now();
        return new TicketsReserved(UUID.randomUUID(), orderId, "concert-1", 2, amountCents, "EUR",
                now.plusSeconds(300), now);
    }

    private static void send(TicketsReserved event) throws Exception {
        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                new StringSerializer(), EventJson.serializer())) {
            producer.send(new ProducerRecord<>(Topics.TICKETS_RESERVED, event.orderId().toString(), event)).get();
        }
    }

    /** Attend `count` PaymentResult pour cette commande (le topic est partage entre les tests). */
    private List<ConsumerRecord<String, Object>> awaitResults(UUID orderId, int count) {
        List<ConsumerRecord<String, Object>> received = new ArrayList<>();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            resultConsumer.poll(Duration.ofMillis(500)).forEach(r -> {
                if (r.value() instanceof PaymentResult p && p.orderId().equals(orderId)) {
                    received.add(r);
                }
            });
            assertThat(received).hasSize(count);
        });
        return received;
    }

    @Test
    void successfulChargeCallsProviderWithOrderIdAsKeyAndPublishesSucceeded() throws Exception {
        PROVIDER.respond(200, "{\"chargeId\":\"ch_1\",\"status\":\"SUCCEEDED\"}");
        UUID orderId = UUID.randomUUID();
        TicketsReserved event = reserved(orderId, 12550);

        send(event);

        ConsumerRecord<String, Object> record = awaitResults(orderId, 1).get(0);
        PaymentResult result = (PaymentResult) record.value();
        assertThat(record.key()).isEqualTo(orderId.toString());
        assertThat(result.status()).isEqualTo(Status.SUCCEEDED);
        assertThat(result.providerChargeId()).isEqualTo("ch_1");
        assertThat(result.failureReason()).isNull();
        assertThat(result.orderId()).isEqualTo(orderId);
        assertThat(result.eventId()).isEqualTo("concert-1");
        assertThat(result.amountCents()).isEqualTo(12550L);
        assertThat(result.currency()).isEqualTo("EUR");
        assertThat(result.messageId()).isNotNull().isNotEqualTo(event.messageId());

        List<Received> calls = PROVIDER.callsFor(orderId);
        assertThat(calls).hasSize(1);
        assertThat(calls.get(0).path()).isEqualTo("/charges");
        assertThat(calls.get(0).idempotencyKey()).isEqualTo(orderId.toString());
        JsonNode body = calls.get(0).body();
        assertThat(body.path("orderId").asString()).isEqualTo(orderId.toString());
        assertThat(body.path("amountCents").asLong()).isEqualTo(12550L);
        assertThat(body.path("currency").asString()).isEqualTo("EUR");
    }

    @Test
    void providerErrorPublishesFailedWithReasonAndDoesNotRetry() throws Exception {
        PROVIDER.respond(500, "{\"error\":\"boom\"}");
        UUID orderId = UUID.randomUUID();

        send(reserved(orderId, 9900));

        PaymentResult result = (PaymentResult) awaitResults(orderId, 1).get(0).value();
        assertThat(result.status()).isEqualTo(Status.FAILED);
        assertThat(result.providerChargeId()).isNull();
        assertThat(result.failureReason()).isNotBlank();
        assertThat(result.orderId()).isEqualTo(orderId);
        assertThat(result.amountCents()).isEqualTo(9900L);
        assertThat(result.currency()).isEqualTo("EUR");
        assertThat(PROVIDER.callsFor(orderId)).hasSize(1);
    }

    @Test
    void providerTimeoutPublishesFailedAndTheConsumerKeepsProcessingNextMessages() throws Exception {
        PROVIDER.hang();
        UUID slowOrder = UUID.randomUUID();

        send(reserved(slowOrder, 7000));

        PaymentResult failed = (PaymentResult) awaitResults(slowOrder, 1).get(0).value();
        assertThat(failed.status()).isEqualTo(Status.FAILED);
        assertThat(failed.providerChargeId()).isNull();
        assertThat(failed.failureReason()).isNotBlank();

        // Le consumer est toujours vivant: une exception ne bloque pas la partition.
        PROVIDER.respond(200, "{\"chargeId\":\"ch_2\",\"status\":\"SUCCEEDED\"}");
        UUID nextOrder = UUID.randomUUID();
        send(reserved(nextOrder, 3000));

        PaymentResult ok = (PaymentResult) awaitResults(nextOrder, 1).get(0).value();
        assertThat(ok.status()).isEqualTo(Status.SUCCEEDED);
        assertThat(ok.providerChargeId()).isEqualTo("ch_2");
    }

    @Test
    void redeliveredOrderAlwaysReusesTheSameIdempotencyKey() throws Exception {
        PROVIDER.respond(200, "{\"chargeId\":\"ch_3\",\"status\":\"SUCCEEDED\"}");
        UUID orderId = UUID.randomUUID();

        // Meme commande, deux messages (messageId differents): simule une re-livraison.
        send(reserved(orderId, 4200));
        send(reserved(orderId, 4200));

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(PROVIDER.callsFor(orderId)).hasSize(2));
        assertThat(PROVIDER.callsFor(orderId))
                .extracting(Received::idempotencyKey)
                .containsOnly(orderId.toString());
    }

    record Received(String path, String idempotencyKey, JsonNode body) {
    }

    /** Provider simule: enregistre chaque appel, reponse configurable (statut fixe ou blocage > timeout). */
    static final class StubProvider {

        private final HttpServer server;
        private final List<Received> calls = new CopyOnWriteArrayList<>();
        private volatile int status = 200;
        private volatile String responseBody = "{}";
        private volatile boolean hanging;
        private volatile CountDownLatch gate = new CountDownLatch(1);

        StubProvider() {
            try {
                server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            // Pool: un handler bloque ne doit pas empecher de servir les requetes suivantes.
            server.setExecutor(Executors.newCachedThreadPool());
            server.createContext("/", this::handle);
            server.start();
        }

        String baseUrl() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        void reset() {
            calls.clear();
            respond(200, "{}");
        }

        void respond(int httpStatus, String body) {
            release();
            this.status = httpStatus;
            this.responseBody = body;
            this.hanging = false;
        }

        /** Les requetes suivantes restent bloquees (bien au-dela de payment.provider.timeout-ms=500). */
        void hang() {
            this.gate = new CountDownLatch(1);
            this.hanging = true;
        }

        void release() {
            gate.countDown();
        }

        void stop() {
            release();
            server.stop(0);
        }

        List<Received> callsFor(UUID orderId) {
            return calls.stream()
                    .filter(c -> orderId.toString().equals(c.body().path("orderId").asString()))
                    .toList();
        }

        private void handle(HttpExchange exchange) throws IOException {
            String raw = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            calls.add(new Received(exchange.getRequestURI().getPath(),
                    exchange.getRequestHeaders().getFirst("Idempotency-Key"),
                    EventJson.mapper().readTree(raw)));
            if (hanging) {
                try {
                    gate.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            byte[] out = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            try {
                exchange.sendResponseHeaders(status, out.length);
                exchange.getResponseBody().write(out);
            } catch (IOException ignored) {
                // le client a deja abandonne (timeout): normal
            } finally {
                exchange.close();
            }
        }
    }
}
