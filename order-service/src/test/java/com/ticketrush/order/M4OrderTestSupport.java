package com.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.Status;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;

/** Real database fixtures and durable assertions shared by the M4 integration tests. */
final class M4OrderTestSupport {
    static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    static final String EVENT = "m4-concert";

    private M4OrderTestSupport() {
    }

    static OrderRequested requested(UUID id) {
        return new OrderRequested(UUID.randomUUID(), id, "m4-" + id, EVENT,
                "m4-customer", 3, START);
    }

    static PaymentResult payment(UUID id, Status status) {
        return new PaymentResult(UUID.randomUUID(), id, EVENT, status, 12_550L, "EUR",
                status == Status.SUCCEEDED ? "charge-" + id : null,
                status == Status.FAILED ? "CARD_DECLINED" : null, START.plusSeconds(121));
    }

    static void seed(OrderRepository repository, JdbcClient jdbc, UUID id, String status) {
        repository.onRequested(requested(id));
        jdbc.sql("UPDATE orders.orders SET status = :status, created_at = :at, updated_at = :at"
                        + " WHERE order_id = :id")
                .param("status", status).param("at", Timestamp.from(START)).param("id", id).update();
    }

    static void requireOutbox(JdbcClient jdbc) {
        assertThat(jdbc.sql("SELECT coalesce(to_regclass('orders.outbox')::text, '<missing>')")
                .query(String.class).single())
                .as("M4 migration must create the approved orders.outbox table")
                .isEqualTo("orders.outbox");
    }

    record OutboxRow(UUID id, String topic, String key, String type, String payload,
                     Instant createdAt, Instant publishedAt) {
    }

    static List<OutboxRow> outbox(JdbcClient jdbc, UUID id) {
        return jdbc.sql("SELECT id, topic, msg_key, type, payload::text, created_at, published_at"
                        + " FROM orders.outbox WHERE payload ->> 'orderId' = :id ORDER BY created_at, id")
                .param("id", id.toString()).query((rs, row) -> new OutboxRow(
                        rs.getObject("id", UUID.class), rs.getString("topic"), rs.getString("msg_key"),
                        rs.getString("type"), rs.getString("payload"),
                        rs.getTimestamp("created_at").toInstant(),
                        rs.getTimestamp("published_at") == null ? null
                                : rs.getTimestamp("published_at").toInstant())).list();
    }

    static Object payload(OutboxRow row) {
        return switch (row.type()) {
            case "OrderConfirmed" -> EventJson.mapper().readValue(
                    row.payload(), com.ticketrush.common.OrderConfirmed.class);
            case "TicketsReleased" -> EventJson.mapper().readValue(
                    row.payload(), com.ticketrush.common.TicketsReleased.class);
            default -> throw new AssertionError("Unexpected outbox type: " + row.type());
        };
    }
}
