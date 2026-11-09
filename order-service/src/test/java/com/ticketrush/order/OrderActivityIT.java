package com.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Tag("m7-red")
class OrderActivityIT extends OrderActivityTestSupport {
    static Stream<Arguments> limits() {
        return Stream.of(Arguments.of("", 20), Arguments.of("?limit=1", 1),
                Arguments.of("?limit=20", 20), Arguments.of("?limit=50", 50));
    }

    @ParameterizedTest
    @MethodSource("limits")
    void AC3_returnsBoundedPublicProjection(String query, int limit) throws Exception {
        seedOrders(51);
        jdbc.update("""
                INSERT INTO orders.outbox (id, topic, msg_key, type, payload, created_at)
                VALUES ('00000000-0000-0000-0000-000000000007', 'orders.confirmed',
                    'read-only-sentinel', 'OrderConfirmed', '{"sentinel":"unchanged"}'::jsonb,
                    timestamptz '2026-01-01 00:00:00+00')
                """);
        List<Map<String, Object>> before = databaseSnapshot();
        List<Map<String, Object>> outboxBefore = jdbc.queryForList("SELECT * FROM orders.outbox ORDER BY id");
        List<PublicOrder> expected = jdbc.query("""
                SELECT order_id, event_id, quantity, status, failure_reason, amount_cents,
                       currency, created_at, updated_at FROM orders.orders
                ORDER BY created_at DESC LIMIT ?
                """, (rs, row) -> new PublicOrder(rs.getString("order_id"), rs.getString("event_id"),
                rs.getInt("quantity"), rs.getString("status"), rs.getString("failure_reason"),
                rs.getLong("amount_cents"), rs.getString("currency"),
                rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant()), limit);

        HttpResponse<String> response = get(query);

        assertThat(databaseSnapshot()).isEqualTo(before);
        assertThat(jdbc.queryForList("SELECT * FROM orders.outbox ORDER BY id")).isEqualTo(outboxBefore);
        JsonNode body = assertActivity(response);
        assertThat(body.get("orders").size()).isEqualTo(limit);
        assertThat(body.get("orders").valueStream().map(this::publicOrder).toList()).containsExactlyElementsOf(expected);
        assertThat(body.get("orders").valueStream().toList()).allSatisfy(order -> {
            assertThat(order.size()).isEqualTo(9);
            assertThat(order.get("quantity").isIntegralNumber()).isTrue();
            assertThat(order.get("amountCents").isIntegralNumber()).isTrue();
        });
        assertThat(response.body()).doesNotContain("customerId", "customer_id", "idempotencyKey",
                "idempotency_key", "PRIVATE-CUSTOMER", "PRIVATE-KEY");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void AC3_emptyAndSingle(int count) throws Exception {
        seedOrders(count);

        JsonNode body = assertActivity(get(""));

        assertThat(body.get("orders").valueStream().map(this::publicOrder).toList())
                .containsExactlyElementsOf(jdbc.query("""
                        SELECT order_id, event_id, quantity, status, failure_reason, amount_cents,
                               currency, created_at, updated_at FROM orders.orders ORDER BY created_at DESC
                        """, (rs, row) -> new PublicOrder(rs.getString("order_id"), rs.getString("event_id"),
                        rs.getInt("quantity"), rs.getString("status"), rs.getString("failure_reason"),
                        rs.getLong("amount_cents"), rs.getString("currency"),
                        rs.getTimestamp("created_at").toInstant(), rs.getTimestamp("updated_at").toInstant())));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "0", "51", "abc", "2147483648"})
    void AC3_invalidLimit(String limit) throws Exception {
        HttpResponse<String> response = get("?limit=" + limit);

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(databaseSnapshot()).isEmpty();
    }

    private PublicOrder publicOrder(JsonNode order) {
        return new PublicOrder(order.get("orderId").asString(), order.get("eventId").asString(),
                order.get("quantity").asInt(), order.get("status").asString(),
                order.get("failureReason").isNull() ? null : order.get("failureReason").asString(),
                order.get("amountCents").asLong(), order.get("currency").asString(),
                Instant.parse(order.get("createdAt").asString()), Instant.parse(order.get("updatedAt").asString()));
    }

    private record PublicOrder(String orderId, String eventId, int quantity, String status,
                               String failureReason, long amountCents, String currency,
                               Instant createdAt, Instant updatedAt) { }
}

// Shared real Spring/HTTP/Postgres fixture. No scheduled writers or live Kafka consumers.
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"order.saga.reaper-enabled=false", "order.outbox.relay-enabled=false",
                "spring.kafka.listener.auto-startup=false"})
abstract class OrderActivityTestSupport {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String ACTIVITY_JDBC_URL = createIsolatedDatabase();

    private static String createIsolatedDatabase() {
        // Other cached Spring contexts have active relays. Share the container, not their database.
        try (Connection connection = OrderTestInfra.connection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE m7_activity");
            return OrderTestInfra.POSTGRES.getJdbcUrl()
                    .replace("/" + OrderTestInfra.POSTGRES.getDatabaseName(), "/m7_activity");
        } catch (SQLException exception) {
            throw new IllegalStateException("Cannot create isolated M7 activity test database", exception);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        OrderTestInfra.registerProperties(registry);
        registry.add("spring.datasource.url", () -> ACTIVITY_JDBC_URL);
    }

    @LocalServerPort int port;
    @Autowired JdbcTemplate jdbc;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    @AfterEach
    void clearFixtures() {
        jdbc.update("DELETE FROM orders.outbox");
        jdbc.update("DELETE FROM orders.orders");
    }

    void seedOrders(int count) {
        jdbc.update("""
                INSERT INTO orders.orders (order_id, event_id, customer_id, idempotency_key, quantity,
                    status, failure_reason, amount_cents, currency, created_at, updated_at)
                SELECT md5('m7-order-' || n)::uuid, 'event-' || n, 'PRIVATE-CUSTOMER-' || n,
                    'PRIVATE-KEY-' || n, 1 + n % 4,
                    (ARRAY['PENDING','RESERVED','CONFIRMED','FAILED','REJECTED'])[1 + n % 5],
                    (ARRAY[NULL,NULL,NULL,'PROVIDER_DECLINED','SOLD_OUT'])[1 + n % 5],
                    3000000000::bigint + n, 'EUR',
                    timestamptz '2026-01-01 00:00:00+00' + n * interval '1 second',
                    timestamptz '2026-01-02 00:00:00+00' - n * interval '1 second'
                FROM generate_series(1, ?) AS n
                """, count);
    }

    List<Map<String, Object>> databaseSnapshot() {
        return jdbc.queryForList("SELECT * FROM orders.orders ORDER BY order_id");
    }

    HttpResponse<String> get(String query) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/orders/activity" + query))
                .timeout(Duration.ofSeconds(10)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    JsonNode assertActivity(HttpResponse<String> response) {
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode body = JSON.readTree(response.body());
        assertThat(body.size()).isEqualTo(3);
        assertThat(body.get("windowSeconds").isIntegralNumber()).isTrue();
        assertThat(body.get("windowSeconds").asLong()).isEqualTo(10);
        assertThat(body.get("ordersInWindow").isIntegralNumber()).isTrue();
        assertThat(body.get("orders").isArray()).isTrue();
        return body;
    }
}
