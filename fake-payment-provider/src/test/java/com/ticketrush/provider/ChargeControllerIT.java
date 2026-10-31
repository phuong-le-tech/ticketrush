package com.ticketrush.provider;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Vrai HTTP sur port aleatoire, rien de mocke. Contrat HTTP vise (code-writer):
 * - POST /charges, header Idempotency-Key obligatoire (absent -> 400), body JSON
 *   {orderId, amountCents, currency} -> 200 {chargeId, status} avec status = SUCCEEDED en M1.
 * - Meme Idempotency-Key -> meme chargeId, aucune nouvelle charge (ConcurrentHashMap + computeIfAbsent,
 *   atomique: c'est la protection contre la double facturation).
 * - GET /charges?idempotencyKey=k -> 200, tableau JSON des charges enregistrees pour cette cle
 *   (vide si aucune). Sert uniquement aux tests pour compter.
 * Config: server.port = 8083 par defaut (pas de variable d'environnement en M1).
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
class ChargeControllerIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    @LocalServerPort
    int port;

    private final HttpClient http = HttpClient.newHttpClient();

    private HttpResponse<String> post(String idempotencyKey, UUID orderId, long amountCents) throws Exception {
        return post(port, idempotencyKey, orderId, amountCents);
    }

    private HttpResponse<String> post(int targetPort, String idempotencyKey, UUID orderId, long amountCents)
            throws Exception {
        String body = "{\"orderId\":\"" + orderId + "\",\"amountCents\":" + amountCents
                + ",\"currency\":\"EUR\"}";
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://localhost:" + targetPort + "/charges"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode charges(String idempotencyKey) throws Exception {
        return charges(port, idempotencyKey);
    }

    private JsonNode charges(int targetPort, String idempotencyKey) throws Exception {
        String query = URLEncoder.encode(idempotencyKey, StandardCharsets.UTF_8);
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + targetPort
                        + "/charges?idempotencyKey=" + query)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    @org.junit.jupiter.api.Tag("m4-red")
    @ParameterizedTest(name = "AC1 rate={0}, status={1}, charges={2}")
    @CsvSource({",SUCCEEDED,1,\\S+,false", "0.0,SUCCEEDED,1,\\S+,false", "1.0,FAILED,0,^$,true"})
    void AC1_rateZeroAndAbsentSucceedOneFailsWithoutCharge(
            String failureRate, String expectedStatus, int expectedCount, String chargeIdPattern,
            boolean expectedNoChargeId) throws Exception {
        StandardEnvironment environment = providerEnvironment(failureRate);
        assertThat(environment.getProperty("FAKE_PROVIDER_FAILURE_RATE")).isEqualTo(failureRate);
        try (ConfigurableApplicationContext provider = new SpringApplicationBuilder(ProviderApplication.class)
                .environment(environment).run("--server.port=0")) {
            int targetPort = ((WebServerApplicationContext) provider).getWebServer().getPort();
            UUID orderId = UUID.randomUUID();
            String key = orderId.toString();
            assertThat(charges(targetPort, key)).isEmpty();

            JsonNode first = chargeReply(targetPort, key, orderId);
            assertThat(first.path("status").asString()).isEqualTo(expectedStatus);
            assertThat(first.path("chargeId").asString("")).matches(chargeIdPattern);
            assertThat(first.path("chargeId").isMissingNode() || first.path("chargeId").isNull())
                    .isEqualTo(expectedNoChargeId);
            assertThat(charges(targetPort, key)).hasSize(expectedCount);

            JsonNode second = chargeReply(targetPort, key, orderId);
            assertThat(second.path("status").asString()).isEqualTo(expectedStatus);
            assertThat(second.path("chargeId")).isEqualTo(first.path("chargeId"));
            assertThat(charges(targetPort, key)).hasSize(expectedCount);

            JsonNode third = chargeReply(targetPort, key, orderId);
            assertThat(third.path("status").asString()).isEqualTo(expectedStatus);
            assertThat(third.path("chargeId")).isEqualTo(first.path("chargeId"));
            assertThat(charges(targetPort, key)).hasSize(expectedCount);
        }
    }

    @org.junit.jupiter.api.Tag("m4-red")
    @ParameterizedTest(name = "AC1 successful retry payload rate={0}")
    @NullSource
    @ValueSource(strings = "0.0")
    void AC1_successfulRetryPreservesExactChargePayload(String failureRate) throws Exception {
        try (ConfigurableApplicationContext provider = new SpringApplicationBuilder(ProviderApplication.class)
                .environment(providerEnvironment(failureRate)).run("--server.port=0")) {
            int targetPort = ((WebServerApplicationContext) provider).getWebServer().getPort();
            UUID orderId = UUID.randomUUID();
            String key = orderId.toString();
            JsonNode first = chargeReply(targetPort, key, orderId);
            assertThat(first.path("status").asString()).isEqualTo("SUCCEEDED");
            assertThat(first.path("chargeId").asString()).isNotBlank();
            assertThat(first.path("orderId").asString()).isEqualTo(key);
            assertThat(first.path("amountCents").asLong()).isEqualTo(5000L);
            assertThat(first.path("currency").asString()).isEqualTo("EUR");
            assertThat(chargeReply(targetPort, key, orderId)).isEqualTo(first);
            assertThat(chargeReply(targetPort, key, orderId)).isEqualTo(first);
            JsonNode recorded = charges(targetPort, key);
            assertThat(recorded).hasSize(1);
            assertThat(recorded.get(0)).isEqualTo(first);
        }
    }

    private static StandardEnvironment providerEnvironment(String failureRate) {
        StandardEnvironment environment = new StandardEnvironment();
        Map<String, Object> variables = failureRate == null ? Map.of()
                : Map.of("FAKE_PROVIDER_FAILURE_RATE", failureRate);
        environment.getPropertySources().replace(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        variables));
        return environment;
    }

    private JsonNode chargeReply(int targetPort, String key, UUID orderId) throws Exception {
        HttpResponse<String> response = post(targetPort, key, orderId, 5000);
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    @Test
    void postChargeReturns200WithSucceededStatusAndAChargeId() throws Exception {
        String key = UUID.randomUUID().toString();

        HttpResponse<String> response = post(key, UUID.fromString(key), 5000);

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = JSON.readTree(response.body());
        assertThat(body.path("status").asString()).isEqualTo("SUCCEEDED");
        assertThat(body.path("chargeId").asString()).isNotBlank();
    }

    @Test
    void twoPostsWithTheSameKeyReturnTheSameChargeIdAndRecordOneCharge() throws Exception {
        String key = UUID.randomUUID().toString();
        UUID orderId = UUID.fromString(key);

        JsonNode first = JSON.readTree(post(key, orderId, 5000).body());
        JsonNode second = JSON.readTree(post(key, orderId, 5000).body());

        assertThat(first.path("chargeId").asString()).isNotBlank();
        assertThat(second.path("chargeId").asString()).isEqualTo(first.path("chargeId").asString());
        assertThat(charges(key)).hasSize(1);
    }

    @Test
    void differentKeysCreateDifferentChargesOneEach() throws Exception {
        String keyA = UUID.randomUUID().toString();
        String keyB = UUID.randomUUID().toString();

        JsonNode a = JSON.readTree(post(keyA, UUID.fromString(keyA), 1000).body());
        JsonNode b = JSON.readTree(post(keyB, UUID.fromString(keyB), 2000).body());

        assertThat(a.path("chargeId").asString()).isNotBlank();
        assertThat(b.path("chargeId").asString()).isNotBlank().isNotEqualTo(a.path("chargeId").asString());
        assertThat(charges(keyA)).hasSize(1);
        assertThat(charges(keyB)).hasSize(1);
    }

    @Test
    void twentyConcurrentPostsWithTheSameKeyCreateExactlyOneCharge() throws Exception {
        String key = UUID.randomUUID().toString();
        UUID orderId = UUID.fromString(key);
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        try {
            List<Future<HttpResponse<String>>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    return post(key, orderId, 5000);
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();

            Set<String> chargeIds = new HashSet<>();
            for (Future<HttpResponse<String>> future : futures) {
                HttpResponse<String> response = future.get(30, TimeUnit.SECONDS);
                assertThat(response.statusCode()).isEqualTo(200);
                chargeIds.add(JSON.readTree(response.body()).path("chargeId").asString());
            }
            assertThat(chargeIds).hasSize(1);
            assertThat(chargeIds.iterator().next()).isNotBlank();
            assertThat(charges(key)).hasSize(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void missingIdempotencyKeyReturns400AndCreatesNoCharge() throws Exception {
        UUID orderId = UUID.randomUUID();

        HttpResponse<String> response = post(null, orderId, 5000);

        assertThat(response.statusCode()).isEqualTo(400);
        // Aucune charge ne doit exister pour la commande, quelle que soit la cle utilisee.
        assertThat(charges(orderId.toString())).isEmpty();
    }
}
