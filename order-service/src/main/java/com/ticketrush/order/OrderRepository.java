package com.ticketrush.order;

import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.ReservationRejected;
import com.ticketrush.common.Status;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Etat de la saga. Choix de conception: chaque message est un upsert conditionnel unique (atomique,
 * sans verrou applicatif). La clause WHERE garantit qu'un etat final n'est jamais ecrase (par un
 * PENDING en retard ou un rejeu), et RETURNING ne renvoie une ligne que si la transition a eu lieu:
 * c'est ce qui permet de publier OrderConfirmed une seule fois.
 */
@Repository
public class OrderRepository {

    /** Ligne renvoyee par une transition effective. */
    public record Saga(String status, String eventId, String customerId, Integer quantity,
                       Long amountCents, String currency) {

        /** Confirmee ET requete connue: on peut publier OrderConfirmed. */
        boolean readyToConfirm() {
            return "CONFIRMED".equals(status) && customerId != null;
        }
    }

    private static final String RETURNING =
            " RETURNING status, event_id, customer_id, quantity, amount_cents, currency";

    private final JdbcClient jdbc;

    OrderRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Remplit les champs de la requete une seule fois; ne touche jamais au statut. */
    Optional<Saga> onRequested(OrderRequested e) {
        return jdbc.sql("""
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
    }

    /** PENDING (ou ligne nouvelle) -> CONFIRMED ou PAYMENT_FAILED; sinon aucune ligne. */
    Optional<Saga> onPayment(PaymentResult e) {
        boolean ok = e.status() == Status.SUCCEEDED;
        return jdbc.sql("""
                INSERT INTO orders.orders (order_id, status, amount_cents, currency, failure_reason)
                VALUES (:id, :status, :amount, :currency, :reason)
                ON CONFLICT (order_id) DO UPDATE SET
                    status = EXCLUDED.status, amount_cents = EXCLUDED.amount_cents,
                    currency = EXCLUDED.currency, failure_reason = EXCLUDED.failure_reason,
                    updated_at = now()
                WHERE orders.orders.status = 'PENDING'""" + RETURNING)
                .param("id", e.orderId()).param("status", ok ? "CONFIRMED" : "PAYMENT_FAILED")
                .param("amount", e.amountCents()).param("currency", e.currency())
                .param("reason", ok ? null : e.failureReason())
                .query(OrderRepository::map).optional();
    }

    /** PENDING (ou ligne nouvelle) -> REJECTED; sinon aucune ligne. */
    Optional<Saga> onRejected(ReservationRejected e) {
        return jdbc.sql("""
                INSERT INTO orders.orders (order_id, status, failure_reason)
                VALUES (:id, 'REJECTED', :reason)
                ON CONFLICT (order_id) DO UPDATE SET
                    status = 'REJECTED', failure_reason = EXCLUDED.failure_reason, updated_at = now()
                WHERE orders.orders.status = 'PENDING'""" + RETURNING)
                .param("id", e.orderId()).param("reason", e.reason())
                .query(OrderRepository::map).optional();
    }

    private static Saga map(ResultSet rs, int row) throws SQLException {
        return new Saga(rs.getString("status"), rs.getString("event_id"), rs.getString("customer_id"),
                (Integer) rs.getObject("quantity"), (Long) rs.getObject("amount_cents"),
                rs.getString("currency"));
    }
}
