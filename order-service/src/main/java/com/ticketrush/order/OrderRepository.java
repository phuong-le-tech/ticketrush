package com.ticketrush.order;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.ReservationRejected;
import com.ticketrush.common.Status;
import com.ticketrush.common.TicketsReleased;
import com.ticketrush.common.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Etat de la saga. Choix de conception: chaque message est un upsert conditionnel unique (atomique,
 * sans verrou applicatif). La clause WHERE garantit qu'un etat final n'est jamais ecrase (par un
 * PENDING en retard ou un rejeu), et RETURNING ne renvoie une ligne que si la transition a eu lieu.
 * The event that follows a transition (OrderConfirmed, or TicketsReleased on failure or timeout)
 * is written to orders.outbox in the same transaction: both commit or neither does, and since only
 * an effective transition writes it, each order gets at most one of them.
 */
@Repository
public class OrderRepository {

    /** Ligne renvoyee par une transition effective. */
    public record Saga(UUID orderId, String status, String eventId, String customerId, Integer quantity,
                       Long amountCents, String currency) {
    }

    static final String CONFIRMED = "CONFIRMED";
    static final String FAILED = "FAILED";
    static final String REASON_PAYMENT_FAILED = "PAYMENT_FAILED";
    static final String REASON_TIMEOUT = "TIMEOUT";

    private static final String RETURNING =
            " RETURNING order_id, status, event_id, customer_id, quantity, amount_cents, currency";

    private final JdbcClient jdbc;
    private Map<String, Counter> outcomes = Map.of();

    @Autowired
    void registerMetrics(MeterRegistry registry) {
        outcomes = Map.of(
                CONFIRMED, registry.counter("ticketrush.saga.outcomes", "outcome", CONFIRMED),
                FAILED, registry.counter("ticketrush.saga.outcomes", "outcome", FAILED),
                "REJECTED", registry.counter("ticketrush.saga.outcomes", "outcome", "REJECTED"));
    }

    private void countAfterCommit(String outcome, int count) {
        // SQL RETURNING identifies effective transitions; commit is the boundary for counting them.
        Counter counter = outcomes.get(outcome);
        if (counter == null || count == 0) return;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                counter.increment(count);
            }
        });
    }

    OrderRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Remplit les champs de la requete une seule fois; ne touche jamais au statut. */
    @Transactional
    Optional<Saga> onRequested(OrderRequested e) {
        Optional<Saga> transition = jdbc.sql("""
                INSERT INTO orders.orders (order_id, idempotency_key, event_id, customer_id, quantity, status)
                VALUES (:id, :key, :eventId, :customerId, :quantity, 'PENDING')
                ON CONFLICT (order_id) DO UPDATE SET
                    idempotency_key = EXCLUDED.idempotency_key, event_id = EXCLUDED.event_id,
                    customer_id = EXCLUDED.customer_id, quantity = EXCLUDED.quantity, updated_at = now()
                WHERE orders.orders.customer_id IS NULL""" + RETURNING)
                .param("id", e.orderId()).param("key", e.idempotencyKey())
                .param("eventId", e.eventId()).param("customerId", e.customerId())
                .param("quantity", e.quantity())
                .query(OrderRepository::map).optional();
        transition.ifPresent(this::enqueueOutcome);
        return transition;
    }

    /** PENDING/RESERVED (ou ligne nouvelle) -> CONFIRMED ou FAILED; sinon aucune ligne. */
    @Transactional
    Optional<Saga> onPayment(PaymentResult e) {
        boolean ok = e.status() == Status.SUCCEEDED;
        Optional<Saga> transition = jdbc.sql("""
                INSERT INTO orders.orders (order_id, status, amount_cents, currency, failure_reason)
                VALUES (:id, :status, :amount, :currency, :reason)
                ON CONFLICT (order_id) DO UPDATE SET
                    status = EXCLUDED.status, amount_cents = EXCLUDED.amount_cents,
                    currency = EXCLUDED.currency, failure_reason = EXCLUDED.failure_reason,
                    updated_at = now()
                WHERE orders.orders.status IN ('PENDING', 'RESERVED')""" + RETURNING)
                .param("id", e.orderId()).param("status", ok ? CONFIRMED : FAILED)
                .param("amount", e.amountCents()).param("currency", e.currency())
                .param("reason", ok ? null : e.failureReason())
                .query(OrderRepository::map).optional();
        transition.ifPresent(saga -> countAfterCommit(saga.status(), 1));
        transition.ifPresent(this::enqueueOutcome);
        return transition;
    }

    /** PENDING/RESERVED (ou ligne nouvelle) -> REJECTED; sinon aucune ligne. Nothing to release. */
    @Transactional
    Optional<Saga> onRejected(ReservationRejected e) {
        Optional<Saga> transition = jdbc.sql("""
                INSERT INTO orders.orders (order_id, status, failure_reason)
                VALUES (:id, 'REJECTED', :reason)
                ON CONFLICT (order_id) DO UPDATE SET
                    status = 'REJECTED', failure_reason = EXCLUDED.failure_reason, updated_at = now()
                WHERE orders.orders.status IN ('PENDING', 'RESERVED')""" + RETURNING)
                .param("id", e.orderId()).param("reason", e.reason())
                .query(OrderRepository::map).optional();
        transition.ifPresent(saga -> countAfterCommit(saga.status(), 1));
        return transition;
    }

    /**
     * Active orders created strictly before the deadline -> FAILED(TIMEOUT), each with its
     * TicketsReleased. SKIP LOCKED: an order a payment is updating right now is left to that
     * payment, so the two never both win. Returns the number of orders expired.
     */
    @Transactional
    int expire(Instant deadline, int limit, Instant now) {
        List<Saga> expired = jdbc.sql("""
                UPDATE orders.orders SET status = 'FAILED', failure_reason = :reason, updated_at = now()
                WHERE status IN ('PENDING', 'RESERVED') AND order_id IN (
                    SELECT order_id FROM orders.orders
                    WHERE status IN ('PENDING', 'RESERVED') AND created_at < :deadline
                    ORDER BY created_at LIMIT :limit
                    FOR UPDATE SKIP LOCKED)""" + RETURNING)
                .param("reason", REASON_TIMEOUT).param("deadline", Timestamp.from(deadline))
                .param("limit", limit)
                .query(OrderRepository::map).list();
        expired.forEach(saga -> enqueueRelease(saga, REASON_TIMEOUT, now));
        countAfterCommit(FAILED, expired.size());
        return expired.size();
    }

    Optional<String> statusOf(UUID orderId) {
        return jdbc.sql("SELECT status FROM orders.orders WHERE order_id = :id")
                .param("id", orderId).query(String.class).optional();
    }

    /**
     * A payment outcome becomes an event only once the request is known (customer, quantity): if
     * the payment came first, the later onRequested transition writes it.
     */
    private void enqueueOutcome(Saga s) {
        if (s.customerId() == null) {
            return;
        }
        if (CONFIRMED.equals(s.status())) {
            enqueue(Topics.ORDERS_CONFIRMED, s.orderId().toString(), new OrderConfirmed(UUID.randomUUID(),
                    s.orderId(), s.eventId(), s.customerId(), s.quantity(), s.amountCents(), s.currency(),
                    Instant.now()));
        } else if (FAILED.equals(s.status())) {
            enqueueRelease(s, REASON_PAYMENT_FAILED, Instant.now());
        }
    }

    private void enqueueRelease(Saga s, String reason, Instant now) {
        enqueue(Topics.TICKETS_RELEASED, s.eventId(),
                new TicketsReleased(UUID.randomUUID(), s.orderId(), s.eventId(), s.quantity(), reason, now));
    }

    private void enqueue(String topic, String key, Object event) {
        jdbc.sql("""
                INSERT INTO orders.outbox (topic, msg_key, type, payload)
                VALUES (:topic, :key, :type, CAST(:payload AS jsonb))""")
                .param("topic", topic).param("key", key).param("type", EventJson.typeIdOf(event.getClass()))
                .param("payload", EventJson.mapper().writeValueAsString(event))
                .update();
    }

    private static Saga map(ResultSet rs, int row) throws SQLException {
        return new Saga(rs.getObject("order_id", UUID.class), rs.getString("status"), rs.getString("event_id"),
                rs.getString("customer_id"), (Integer) rs.getObject("quantity"),
                (Long) rs.getObject("amount_cents"), rs.getString("currency"));
    }
}
