package com.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.OrderRequested;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.ReservationRejected;
import com.ticketrush.common.Status;
import com.ticketrush.common.Topics;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Saga complete avec Kafka et Postgres reels, sans mock. Contrat: voir OrderTestInfra.
 * Un UUID frais par test isole les tests entre eux (DB et topics partages).
 */
@SpringBootTest
class OrderSagaIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        OrderTestInfra.registerProperties(registry);
    }

    // ---- fabriques d'evenements ----

    private static OrderRequested requested(UUID orderId) {
        return new OrderRequested(UUID.randomUUID(), orderId, "idem-" + orderId, "event-1",
                "customer-1", 3, Instant.now());
    }

    private static PaymentResult succeeded(UUID orderId) {
        return new PaymentResult(UUID.randomUUID(), orderId, "event-1", Status.SUCCEEDED,
                12_550L, "EUR", "ch_" + orderId, null, Instant.now());
    }

    private static PaymentResult failed(UUID orderId) {
        return new PaymentResult(UUID.randomUUID(), orderId, "event-1", Status.FAILED,
                12_550L, "EUR", null, "CARD_DECLINED", Instant.now());
    }

    private static ReservationRejected rejected(UUID orderId) {
        return new ReservationRejected(UUID.randomUUID(), orderId, "event-1", "SOLD_OUT", Instant.now());
    }

    private static void sendRequested(OrderRequested e) {
        OrderTestInfra.send(Topics.ORDERS_REQUESTED, e.orderId(), e);
    }

    private static void sendPayment(PaymentResult e) {
        OrderTestInfra.send(Topics.PAYMENTS_RESULT, e.orderId(), e);
    }

    private static void sendRejected(ReservationRejected e) {
        OrderTestInfra.send(Topics.ORDERS_REJECTED, e.orderId(), e);
    }

    // ---- assertions asynchrones ----

    private static void awaitStatus(UUID orderId, String status) {
        await().atMost(TIMEOUT).untilAsserted(
                () -> assertThat(OrderTestInfra.statusOf(orderId)).isEqualTo(status));
    }

    private static void awaitConfirmed(UUID orderId, int count) {
        await().atMost(TIMEOUT).untilAsserted(
                () -> assertThat(OrderTestInfra.confirmedFor(orderId)).hasSize(count));
    }

    private static void assertNoConfirmed(UUID orderId) {
        OrderTestInfra.awaitIdle();
        assertThat(OrderTestInfra.confirmedFor(orderId)).isEmpty();
    }

    // ---- 1. OrderRequested ----

    @Test
    void orderRequestedCreatesOnePendingRowAndReplaysDoNotDuplicate() {
        UUID orderId = UUID.randomUUID();
        OrderRequested first = requested(orderId);

        sendRequested(first);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            List<OrderTestInfra.Row> rows = OrderTestInfra.rows(orderId);
            assertThat(rows).hasSize(1);
            OrderTestInfra.Row row = rows.get(0);
            assertThat(row.status()).isEqualTo("PENDING");
            assertThat(row.idempotencyKey()).isEqualTo(first.idempotencyKey());
            assertThat(row.eventId()).isEqualTo("event-1");
            assertThat(row.customerId()).isEqualTo("customer-1");
            assertThat(row.quantity()).isEqualTo(3);
        });

        sendRequested(first);
        sendRequested(new OrderRequested(UUID.randomUUID(), orderId, first.idempotencyKey(),
                "event-1", "customer-1", 3, Instant.now()));
        OrderTestInfra.awaitIdle();

        assertThat(OrderTestInfra.rows(orderId)).hasSize(1);
        assertThat(OrderTestInfra.statusOf(orderId)).isEqualTo("PENDING");
    }

    // ---- 2. PENDING + SUCCEEDED ----

    @Test
    void succeededOnPendingOrderConfirmsAndPublishesOrderConfirmedOnce() {
        UUID orderId = UUID.randomUUID();
        OrderRequested request = requested(orderId);
        sendRequested(request);
        awaitStatus(orderId, "PENDING");

        sendPayment(succeeded(orderId));

        awaitStatus(orderId, "CONFIRMED");
        OrderTestInfra.Row row = OrderTestInfra.rows(orderId).get(0);
        assertThat(row.amountCents()).isEqualTo(12_550L);
        assertThat(row.currency()).isEqualTo("EUR");

        awaitConfirmed(orderId, 1);
        OrderTestInfra.awaitIdle();
        List<ConsumerRecord<String, Object>> published = OrderTestInfra.confirmedFor(orderId);
        assertThat(published).hasSize(1);
        ConsumerRecord<String, Object> record = published.get(0);
        assertThat(record.key()).isEqualTo(orderId.toString());
        assertThat(new String(record.headers().lastHeader("__TypeId__").value(), StandardCharsets.UTF_8))
                .isEqualTo("OrderConfirmed");
        assertThat(record.value()).isInstanceOf(OrderConfirmed.class);
        OrderConfirmed event = (OrderConfirmed) record.value();
        assertThat(event.orderId()).isEqualTo(orderId);
        assertThat(event.eventId()).isEqualTo("event-1");
        assertThat(event.customerId()).isEqualTo("customer-1");
        assertThat(event.quantity()).isEqualTo(3);
        assertThat(event.amountCents()).isEqualTo(12_550L);
        assertThat(event.currency()).isEqualTo("EUR");
        assertThat(event.messageId()).isNotNull();
        assertThat(event.occurredAt()).isNotNull();
    }

    // ---- 3. Rejeux apres confirmation ----

    @Test
    void replaysAfterConfirmationNeverPublishASecondOrderConfirmed() {
        UUID orderId = UUID.randomUUID();
        OrderRequested request = requested(orderId);
        PaymentResult payment = succeeded(orderId);
        sendRequested(request);
        awaitStatus(orderId, "PENDING");
        sendPayment(payment);
        awaitConfirmed(orderId, 1);

        sendPayment(payment);
        sendPayment(new PaymentResult(UUID.randomUUID(), orderId, "event-1", Status.SUCCEEDED,
                12_550L, "EUR", payment.providerChargeId(), null, Instant.now()));
        // Risque cible: l'upsert d'OrderRequested renvoie CONFIRMED par RETURNING et republie.
        sendRequested(request);
        sendRequested(requested(orderId));
        OrderTestInfra.awaitIdle();

        assertThat(OrderTestInfra.confirmedFor(orderId)).hasSize(1);
        assertThat(OrderTestInfra.statusOf(orderId)).isEqualTo("CONFIRMED");
        assertThat(OrderTestInfra.rows(orderId)).hasSize(1);
    }

    // ---- 4. FAILED ----

    @Test
    void failedPaymentMarksPaymentFailedWithReasonAndPublishesNothing() {
        UUID orderId = UUID.randomUUID();
        sendRequested(requested(orderId));
        awaitStatus(orderId, "PENDING");

        sendPayment(failed(orderId));

        awaitStatus(orderId, "PAYMENT_FAILED");
        assertThat(OrderTestInfra.rows(orderId).get(0).failureReason()).isEqualTo("CARD_DECLINED");
        assertNoConfirmed(orderId);
    }

    // ---- 5. ReservationRejected ----

    @Test
    void reservationRejectedMarksRejected() {
        UUID orderId = UUID.randomUUID();
        sendRequested(requested(orderId));
        awaitStatus(orderId, "PENDING");

        sendRejected(rejected(orderId));

        awaitStatus(orderId, "REJECTED");
        assertNoConfirmed(orderId);
    }

    // ---- 6. SUCCEEDED avant OrderRequested ----

    @Test
    void succeededBeforeOrderRequestedStaysConfirmedAndPublishesOnlyOnceTheRequestArrives() {
        UUID orderId = UUID.randomUUID();
        OrderRequested request = requested(orderId);

        sendPayment(succeeded(orderId));

        awaitStatus(orderId, "CONFIRMED");
        // Sans les champs de la requete (customerId...), rien ne peut etre publie.
        assertNoConfirmed(orderId);

        sendRequested(request);

        awaitConfirmed(orderId, 1);
        OrderTestInfra.awaitIdle();
        assertThat(OrderTestInfra.statusOf(orderId)).isEqualTo("CONFIRMED");
        OrderTestInfra.Row row = OrderTestInfra.rows(orderId).get(0);
        assertThat(row.idempotencyKey()).isEqualTo(request.idempotencyKey());
        assertThat(row.eventId()).isEqualTo("event-1");
        assertThat(row.customerId()).isEqualTo("customer-1");
        assertThat(row.quantity()).isEqualTo(3);
        assertThat(row.amountCents()).isEqualTo(12_550L);
        assertThat(row.currency()).isEqualTo("EUR");

        List<ConsumerRecord<String, Object>> published = OrderTestInfra.confirmedFor(orderId);
        assertThat(published).hasSize(1);
        OrderConfirmed event = (OrderConfirmed) published.get(0).value();
        assertThat(event.orderId()).isEqualTo(orderId);
        assertThat(event.eventId()).isEqualTo("event-1");
        assertThat(event.customerId()).isEqualTo("customer-1");
        assertThat(event.quantity()).isEqualTo(3);
        assertThat(event.amountCents()).isEqualTo(12_550L);
        assertThat(event.currency()).isEqualTo("EUR");
    }

    // ---- 7. FAILED / REJECTED avant OrderRequested ----

    @ParameterizedTest
    @ValueSource(strings = {"PAYMENT_FAILED", "REJECTED"})
    void terminalStateReceivedBeforeOrderRequestedIsNotOverwrittenByThePendingRequest(String terminal) {
        UUID orderId = UUID.randomUUID();
        if (terminal.equals("PAYMENT_FAILED")) {
            sendPayment(failed(orderId));
        } else {
            sendRejected(rejected(orderId));
        }
        awaitStatus(orderId, terminal);

        sendRequested(requested(orderId));
        OrderTestInfra.awaitIdle();

        assertThat(OrderTestInfra.statusOf(orderId)).isEqualTo(terminal);
        assertThat(OrderTestInfra.rows(orderId)).hasSize(1);
        assertNoConfirmed(orderId);
    }

    // ---- 8. Etat terminal colle ----

    @Test
    void succeededAfterPaymentFailedDoesNotResurrectTheOrder() {
        UUID orderId = UUID.randomUUID();
        sendRequested(requested(orderId));
        awaitStatus(orderId, "PENDING");
        sendPayment(failed(orderId));
        awaitStatus(orderId, "PAYMENT_FAILED");

        sendPayment(succeeded(orderId));
        OrderTestInfra.awaitIdle();

        assertThat(OrderTestInfra.statusOf(orderId)).isEqualTo("PAYMENT_FAILED");
        assertNoConfirmed(orderId);
    }

    // ---- 9. Concurrence ----

    @Test
    void thirtyOrdersWithRequestAndPaymentInRandomOrderAreAllConfirmedExactlyOnce() throws Exception {
        int count = 30;
        List<UUID> ids = new ArrayList<>();
        List<Supplier<Void>> sends = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            UUID orderId = UUID.randomUUID();
            ids.add(orderId);
            sends.add(() -> {
                sendRequested(requested(orderId));
                return null;
            });
            sends.add(() -> {
                sendPayment(succeeded(orderId));
                return null;
            });
        }
        Collections.shuffle(sends);

        ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (Supplier<Void> s : sends) {
                futures.add(pool.submit(s::get));
            }
            for (Future<Void> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdown();
        }

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            for (UUID id : ids) {
                List<OrderTestInfra.Row> rows = OrderTestInfra.rows(id);
                assertThat(rows).hasSize(1);
                assertThat(rows.get(0).status()).isEqualTo("CONFIRMED");
                assertThat(rows.get(0).customerId()).isEqualTo("customer-1");
                assertThat(OrderTestInfra.confirmedFor(id)).hasSize(1);
            }
        });
        OrderTestInfra.awaitIdle();
        for (UUID id : ids) {
            assertThat(OrderTestInfra.confirmedFor(id)).hasSize(1);
            assertThat(OrderTestInfra.confirmedFor(id).get(0).key()).isEqualTo(id.toString());
        }
    }
}
