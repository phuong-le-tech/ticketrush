package com.ticketrush.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Tag("m7-red")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.kafka.bootstrap-servers=localhost:1",
                "spring.kafka.listener.auto-startup=false"})
class StockEndpointIT {
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:8.10.2").withExposedPorts(6379);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Instant RESERVED_UNTIL = Instant.parse("2030-01-01T00:00:00Z");
    static { REDIS.start(); }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @LocalServerPort int port;
    @Autowired StringRedisTemplate redis;
    @Autowired InventoryRepository inventory;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void clearFixtures() {
        redis.execute((org.springframework.data.redis.core.RedisCallback<Void>) connection -> {
            connection.serverCommands().flushDb();
            return null;
        });
    }

    @Test
    void AC1_readsCurrentStockAfterReservations() throws Exception {
        inventory.seedIfAbsent("concert-1", 12, 5000, "EUR");
        assertStock(get("concert-1"), "concert-1", 12);

        assertThat(inventory.reserve("concert-1", UUID.fromString("00000000-0000-0000-0000-000000000001"),
                2, RESERVED_UNTIL).reserved()).isTrue();
        assertThat(inventory.reserve("concert-1", UUID.fromString("00000000-0000-0000-0000-000000000002"),
                3, RESERVED_UNTIL).reserved()).isTrue();

        assertStock(get("concert-1"), "concert-1", 7);
    }

    static Stream<Arguments> boundaries() {
        return Stream.of(Arguments.of("x", 0L), Arguments.of("e".repeat(255), 1L),
                Arguments.of("large-stock", 2147483648L));
    }

    @ParameterizedTest
    @MethodSource("boundaries")
    void AC1_acceptsBoundaries(String eventId, long stock) throws Exception {
        inventory.seedIfAbsent(eventId, stock, 5000, "EUR");

        HttpResponse<String> response = get(eventId);

        assertStock(response, eventId, stock);
    }

    static Stream<Arguments> invalidIds() {
        return Stream.of(Arguments.of("unknown", 404), Arguments.of("%20%20%20", 400),
                Arguments.of("x".repeat(256), 400));
    }

    @ParameterizedTest
    @MethodSource("invalidIds")
    void AC1_rejectsInvalidAndUnknown(String eventId, int expectedStatus) throws Exception {
        HttpResponse<String> response = get(eventId);

        assertThat(response.statusCode()).isEqualTo(expectedStatus);
        assertThat(redis.hasKey("inventory:event:" + eventId)).isFalse();
    }

    @Test
    void AC2_repeatedReadsNeverWrite() throws Exception {
        inventory.seedIfAbsent("concert-1", 12, 5000, "EUR");
        inventory.reserve("concert-1", UUID.fromString("00000000-0000-0000-0000-000000000003"),
                2, RESERVED_UNTIL);
        redis.opsForValue().set("unrelated:sentinel", "keep-me");
        Map<String, List<Object>> before = snapshot();

        List<CompletableFuture<HttpResponse<String>>> pending = IntStream.range(0, 24)
                .mapToObj(i -> List.of("concert-1", "unknown", "%20%20%20", "x".repeat(256)).get(i % 4))
                .map(id -> http.sendAsync(request(id), HttpResponse.BodyHandlers.ofString())).toList();
        List<Integer> statuses = pending.stream().map(CompletableFuture::join)
                .map(HttpResponse::statusCode).toList();

        assertThat(snapshot()).isEqualTo(before);
        assertThat(redis.hasKey("inventory:event:unknown")).isFalse();
        assertThat(statuses).containsExactly(200, 404, 400, 400, 200, 404, 400, 400,
                200, 404, 400, 400, 200, 404, 400, 400, 200, 404, 400, 400, 200, 404, 400, 400);
    }

    // DUMP captures each complete Redis value, independent of hash iteration order.
    private Map<String, List<Object>> snapshot() {
        Map<String, List<Object>> result = new TreeMap<>();
        redis.keys("*").forEach(key -> result.put(key, List.of(redis.type(key),
                java.util.Base64.getEncoder().encodeToString(redis.dump(key)))));
        return result;
    }

    private HttpRequest request(String eventId) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/events/" + eventId + "/stock"))
                .timeout(Duration.ofSeconds(10)).GET().build();
    }

    private HttpResponse<String> get(String eventId) throws Exception {
        return http.send(request(eventId), HttpResponse.BodyHandlers.ofString());
    }

    private void assertStock(HttpResponse<String> response, String eventId, long stock) {
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = JSON.readTree(response.body());
        assertThat(body.size()).isEqualTo(2);
        assertThat(body.get("eventId").asString()).isEqualTo(eventId);
        assertThat(body.get("stock").isIntegralNumber()).isTrue();
        assertThat(body.get("stock").asLong()).isEqualTo(stock);
    }
}
