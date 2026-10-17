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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
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

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private HttpResponse<String> post(String idempotencyKey, UUID orderId, long amountCents) throws Exception {
        String body = "{\"orderId\":\"" + orderId + "\",\"amountCents\":" + amountCents
                + ",\"currency\":\"EUR\"}";
        HttpRequest.Builder request = HttpRequest.newBuilder(uri("/charges"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode charges(String idempotencyKey) throws Exception {
        String query = URLEncoder.encode(idempotencyKey, StandardCharsets.UTF_8);
        HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(uri("/charges?idempotencyKey=" + query)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
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
