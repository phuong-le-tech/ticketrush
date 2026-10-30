package com.ticketrush.order;

import static com.ticketrush.order.M4OrderTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.Status;
import com.ticketrush.common.TicketsReleased;
import com.ticketrush.common.Topics;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Clock is the only fake; competing transitions use real Postgres transactions. */
@SpringBootTest
@org.junit.jupiter.api.Tag("m4-red")
class SagaReaperIT {
    @Autowired OrderRepository repository;
    @Autowired JdbcClient jdbc;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        OrderTestInfra.registerProperties(registry);
    }

    @ParameterizedTest(name = "AC3 {0} age {1}s remains active")
    @CsvSource({"PENDING,119", "PENDING,120", "RESERVED,119", "RESERVED,120"})
    void AC3_reaperDoesNotExpireAtOrBeforeDeadline(String initial, long ageSeconds) {
        requireOutbox(jdbc);
        UUID id = UUID.randomUUID();
        seed(repository, jdbc, id, initial);

        reaperAt(ageSeconds).reapExpired();

        assertThat(OrderTestInfra.statusOf(id)).isEqualTo(initial);
        assertThat(outbox(jdbc, id)).isEmpty();
    }

    @ParameterizedTest(name = "AC3 timeout first from {0}")
    @ValueSource(strings = {"PENDING", "RESERVED"})
    void AC3_timeoutFirstLateSuccessAndReplayNeverConfirm(String initial) {
        requireOutbox(jdbc);
        UUID id = UUID.randomUUID();
        seed(repository, jdbc, id, initial);
        SagaReaper expired = reaperAt(121);

        expired.reapExpired();
        assertTimeout(id);
        var terminal = OrderTestInfra.rows(id);
        var timeoutEvent = outbox(jdbc, id);
        var late = payment(id, Status.SUCCEEDED);
        repository.onPayment(late);
        repository.onPayment(late);
        repository.onPayment(payment(id, Status.SUCCEEDED));
        expired.reapExpired();

        assertTimeout(id);
        assertThat(OrderTestInfra.rows(id)).isEqualTo(terminal);
        assertThat(outbox(jdbc, id)).isEqualTo(timeoutEvent);
    }

    @ParameterizedTest(name = "AC3 payment first from {0}")
    @ValueSource(strings = {"PENDING", "RESERVED"})
    void AC3_paymentFirstReaperAndFailureNeverOverwriteConfirmation(String initial) {
        requireOutbox(jdbc);
        UUID id = UUID.randomUUID();
        seed(repository, jdbc, id, initial);

        repository.onPayment(payment(id, Status.SUCCEEDED));
        assertConfirmed(id);
        var terminal = OrderTestInfra.rows(id);
        var confirmation = outbox(jdbc, id);
        reaperAt(121).reapExpired();
        repository.onPayment(payment(id, Status.FAILED));

        assertConfirmed(id);
        assertThat(OrderTestInfra.rows(id)).isEqualTo(terminal);
        assertThat(outbox(jdbc, id)).isEqualTo(confirmation);
    }

    @ParameterizedTest(name = "AC3 race from {0}, attempt {1}")
    @CsvSource({
        "PENDING,1", "PENDING,2", "PENDING,3", "PENDING,4",
        "PENDING,5", "PENDING,6", "PENDING,7", "PENDING,8",
        "PENDING,9", "PENDING,10", "PENDING,11", "PENDING,12",
        "RESERVED,1", "RESERVED,2", "RESERVED,3", "RESERVED,4",
        "RESERVED,5", "RESERVED,6", "RESERVED,7", "RESERVED,8",
        "RESERVED,9", "RESERVED,10", "RESERVED,11", "RESERVED,12"
    })
    void AC3_reaperPaymentRaceProducesOneTerminalOutcomeLateSuccessNeverConfirmsTimeout(
            String initial, int attempt) throws Exception {
        requireOutbox(jdbc);
        UUID id = UUID.randomUUID();
        seed(repository, jdbc, id, initial);
        SagaReaper expired = reaperAt(121);
        CyclicBarrier start = new CyclicBarrier(2);
        try (var pool = Executors.newFixedThreadPool(2)) {
            var reaping = pool.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                expired.reapExpired();
                return null;
            });
            var paying = pool.submit(() -> {
                start.await(10, TimeUnit.SECONDS);
                repository.onPayment(payment(id, Status.SUCCEEDED));
                return null;
            });
            reaping.get(20, TimeUnit.SECONDS);
            paying.get(20, TimeUnit.SECONDS);
        }

        var terminal = OrderTestInfra.rows(id);
        var events = outbox(jdbc, id);
        assertThat(terminal).hasSize(1);
        assertThat(events).hasSize(1);
        var order = terminal.getFirst();
        var row = events.getFirst();
        var body = EventJson.mapper().readTree(row.payload());
        // Correlate the complete outcome, not independent sets that permit mixed states.
        assertThat(tuple(order.status(), order.failureReason(), row.topic(), row.key(), row.type(),
                body.path("reason").asString(null)))
                .isIn(tuple("FAILED", "TIMEOUT", Topics.TICKETS_RELEASED, EVENT, "TicketsReleased", "TIMEOUT"),
                        tuple("CONFIRMED", null, Topics.ORDERS_CONFIRMED, id.toString(), "OrderConfirmed", null));
        assertThat(row.id()).isNotNull();
        assertThat(row.createdAt()).isNotNull();
        assertThat(body.path("orderId").asString()).isEqualTo(id.toString());
        assertThat(body.path("eventId").asString()).isEqualTo(EVENT);
        assertThat(body.path("quantity").asInt()).isEqualTo(3);
        assertThat(body.path("messageId").asString()).isNotBlank();
        assertThat(body.path("occurredAt").asString()).isNotBlank();

        repository.onPayment(payment(id, Status.SUCCEEDED));
        expired.reapExpired();
        assertThat(OrderTestInfra.rows(id)).isEqualTo(terminal);
        assertThat(outbox(jdbc, id)).isEqualTo(events);
    }

    private SagaReaper reaperAt(long seconds) {
        return new SagaReaper(repository, Clock.fixed(START.plusSeconds(seconds), ZoneOffset.UTC));
    }

    private void assertTimeout(UUID id) {
        assertThat(OrderTestInfra.statusOf(id)).isEqualTo("FAILED");
        assertThat(OrderTestInfra.rows(id).getFirst().failureReason()).isEqualTo("TIMEOUT");
        var rows = outbox(jdbc, id);
        assertThat(rows).hasSize(1);
        var row = rows.getFirst();
        assertThat(row.topic()).isEqualTo(Topics.TICKETS_RELEASED);
        assertThat(row.key()).isEqualTo(EVENT);
        assertThat(row.type()).isEqualTo("TicketsReleased");
        assertThat(payload(row)).isInstanceOfSatisfying(TicketsReleased.class, event -> {
            assertThat(event.orderId()).isEqualTo(id);
            assertThat(event.eventId()).isEqualTo(EVENT);
            assertThat(event.quantity()).isEqualTo(3);
            assertThat(event.reason()).isEqualTo("TIMEOUT");
            assertThat(event.messageId()).isNotNull();
            assertThat(event.occurredAt()).isNotNull();
        });
    }

    private void assertConfirmed(UUID id) {
        assertThat(OrderTestInfra.statusOf(id)).isEqualTo("CONFIRMED");
        var rows = outbox(jdbc, id);
        assertThat(rows).hasSize(1);
        assertThat(rows.getFirst().topic()).isEqualTo(Topics.ORDERS_CONFIRMED);
        assertThat(rows.getFirst().key()).isEqualTo(id.toString());
        assertThat(rows.getFirst().type()).isEqualTo("OrderConfirmed");
        assertThat(payload(rows.getFirst())).isInstanceOfSatisfying(OrderConfirmed.class,
                event -> assertThat(event.orderId()).isEqualTo(id));
    }
}
