package com.ticketrush.e2e;

import static com.ticketrush.e2e.M4FailureSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.PaymentResult;
import com.ticketrush.common.Status;
import com.ticketrush.common.TicketsReleased;
import com.ticketrush.common.Topics;
import com.ticketrush.order.OrderRepository;
import com.ticketrush.order.OrderServiceApplication;
import com.ticketrush.order.SagaReaper;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** M4 acceptance with real Kafka, Postgres, Redis and HTTP payment provider. */
@org.junit.jupiter.api.Tag("m4-red")
class FailureE2ETest {

    @Test
    void AC1_rateOneFailsPaymentAndRestoresStock_AC9_realContainers() throws Exception {
        try (var stack = start("ac1-failure", 3, "1.0")) {
            UUID id = buy(stack, "ac1-failure", "declined-buyer");

            awaitStatus(stack, id, "FAILED");
            awaitStock(stack, "ac1-failure", 3);
            awaitOutboxPublished(stack);
            stack.awaitQuiescent(SETTLE);
            awaitReleaseConsumed(stack);

            assertThat(stack.statusCounts("ac1-failure")).containsOnly(Map.entry("FAILED", 1L));
            assertThat(stack.totalOrderRows()).isEqualTo(1);
            assertThat(stack.chargeCount(id)).isZero();
            assertThat(records(stack, Topics.ORDERS_CONFIRMED, id)).isEmpty();
            assertThat(records(stack, Topics.PAYMENTS_RESULT, id)).singleElement().satisfies(record -> {
                PaymentResult payment = EventJson.mapper().readValue(record.value(), PaymentResult.class);
                assertThat(payment.status()).isEqualTo(Status.FAILED);
                assertThat(payment.providerChargeId()).isNull();
                assertThat(payment.failureReason()).isNotBlank();
            });
            assertThat(records(stack, Topics.TICKETS_RELEASED, id)).singleElement().satisfies(record -> {
                TicketsReleased release = EventJson.mapper().readValue(record.value(), TicketsReleased.class);
                assertThat(record.key()).isEqualTo("ac1-failure");
                assertThat(release.orderId()).isEqualTo(id);
                assertThat(release.eventId()).isEqualTo("ac1-failure");
                assertThat(release.quantity()).isEqualTo(1);
                assertThat(release.reason()).isEqualTo("PAYMENT_FAILED");
            });
            assertThat(outboxCount(stack, id, Topics.TICKETS_RELEASED)).isEqualTo(1);
        }
    }

    @ParameterizedTest(name = "AC2 failed payment and release replay copies={0}")
    @ValueSource(ints = {1, 10})
    void AC2_oneAndTenConcurrentFailedReplaysRestoreOnce(int copies) throws Exception {
        try (var stack = start("ac2-replay", 4, "1.0")) {
            UUID id = buy(stack, "ac2-replay", "replayed-buyer");
            awaitStatus(stack, id, "FAILED");
            awaitStock(stack, "ac2-replay", 4);
            awaitOutboxPublished(stack);
            stack.awaitQuiescent(SETTLE);
            awaitReleaseConsumed(stack);
            var payments = records(stack, Topics.PAYMENTS_RESULT, id);
            var releases = records(stack, Topics.TICKETS_RELEASED, id);
            assertThat(payments).hasSize(1);
            assertThat(releases).hasSize(1);
            var captured = new ArrayList<>(payments);
            captured.addAll(releases);

            replayConcurrently(stack, captured, copies);
            stack.awaitQuiescent(SETTLE);
            awaitReleaseConsumed(stack);
            awaitOutboxPublished(stack);

            assertThat(stack.stock("ac2-replay")).isEqualTo("4");
            assertThat(stack.orderStatus(id)).isEqualTo("FAILED");
            assertThat(stack.totalOrderRows()).isEqualTo(1);
            assertThat(stack.chargeCount(id)).isZero();
            assertThat(records(stack, Topics.ORDERS_CONFIRMED, id)).isEmpty();
            assertThat(outboxCount(stack, id, Topics.TICKETS_RELEASED)).isEqualTo(1);
            assertThat(records(stack, Topics.TICKETS_RELEASED, id)).hasSize(copies + 1);
        }
    }

    @ParameterizedTest(name = "AC3 timeout initial state={0}")
    @ValueSource(strings = {"PENDING", "RESERVED"})
    void AC3_pendingReservedAt120StayAt121FailRestoreOnceConfirmedNeverReaped(String initial) throws Exception {
        try (var stack = start("ac3-timeout", 3, "0.0")) {
            UUID confirmed = buy(stack, "ac3-timeout", "confirmed-buyer");
            awaitStatus(stack, confirmed, "CONFIRMED");
            awaitOutboxPublished(stack);
            stack.awaitQuiescent(SETTLE);
            awaitReleaseConsumed(stack);
            stopPayment(stack);
            UUID expired = buy(stack, "ac3-timeout", "timeout-buyer");
            awaitStock(stack, "ac3-timeout", 1);
            await().atMost(SETTLE).untilAsserted(() -> assertThat(stack.orderStatus(expired)).isNotNull());
            setSagaAgeAndStatus(stack, expired, initial);
            setSagaAgeAndStatus(stack, confirmed, "CONFIRMED");
            var repository = stack.context(OrderServiceApplication.class).getBean(OrderRepository.class);
            var boundaryRef = new java.util.concurrent.atomic.AtomicReference<SagaReaper>();
            var afterDeadlineRef = new java.util.concurrent.atomic.AtomicReference<SagaReaper>();
            assertThatCode(() -> {
                boundaryRef.set(new SagaReaper(repository, Clock.fixed(START.plusSeconds(120), ZoneOffset.UTC)));
                afterDeadlineRef.set(new SagaReaper(repository, Clock.fixed(START.plusSeconds(121), ZoneOffset.UTC)));
            }).as("production saga reaper must be executable, not a compile-only contract")
                    .doesNotThrowAnyException();
            var boundary = boundaryRef.get();
            var afterDeadline = afterDeadlineRef.get();

            boundary.reapExpired();

            assertThat(stack.orderStatus(expired)).isEqualTo(initial);
            assertThat(stack.stock("ac3-timeout")).isEqualTo("1");
            assertThat(outboxCount(stack, expired, Topics.TICKETS_RELEASED)).isZero();
            assertThat(stack.orderStatus(confirmed)).isEqualTo("CONFIRMED");

            afterDeadline.reapExpired();
            awaitStatus(stack, expired, "FAILED");
            awaitStock(stack, "ac3-timeout", 2);
            awaitOutboxPublished(stack);
            var release = records(stack, Topics.TICKETS_RELEASED, expired);
            assertThat(release).singleElement().satisfies(record -> {
                TicketsReleased event = EventJson.mapper().readValue(record.value(), TicketsReleased.class);
                assertThat(record.key()).isEqualTo("ac3-timeout");
                assertThat(event.reason()).isEqualTo("TIMEOUT");
                assertThat(event.orderId()).isEqualTo(expired);
                assertThat(event.quantity()).isEqualTo(1);
            });

            afterDeadline.reapExpired();
            replayConcurrently(stack, release, 10);
            awaitOutboxPublished(stack);
            // Payment is stopped, so wait for the inventory offsets rather than global quiescence.
            awaitReleaseConsumed(stack);
            assertThat(stack.stock("ac3-timeout")).isEqualTo("2");

            assertThat(stack.orderStatus(expired)).isEqualTo("FAILED");
            assertThat(stack.orderStatus(confirmed)).isEqualTo("CONFIRMED");
            assertThat(outboxCount(stack, expired, Topics.TICKETS_RELEASED)).isEqualTo(1);
            assertThat(outboxCount(stack, confirmed, Topics.TICKETS_RELEASED)).isZero();
            assertThat(records(stack, Topics.ORDERS_CONFIRMED, confirmed)).hasSize(1);
            assertThat(records(stack, Topics.ORDERS_CONFIRMED, expired)).isEmpty();
            assertThat(stack.chargeCount(confirmed)).isEqualTo(1);
            assertThat(stack.chargeCount(expired)).isZero();
            assertThat(stack.totalOrderRows()).isEqualTo(2);
        }
    }

    @ParameterizedTest(name = "AC6 provider transport fault={0}")
    @CsvSource({"HTTP500, 1", "LOST_REPLY, 2"})
    void AC6_transient500AndChargedButLostReplyRetrySameKeyOneChargeOneConfirmation(
            Fault fault, int minimumUpstreamCalls) throws Exception {
        try (var stack = start("ac6-retry", 1, "0.0");
             var proxy = new FaultProxy(stack, fault)) {
            UUID id = buy(stack, "ac6-retry", "retry-buyer");

            awaitStatus(stack, id, "CONFIRMED");
            awaitOutboxPublished(stack);
            stack.awaitQuiescent(SETTLE);
            awaitReleaseConsumed(stack);

            assertThat(proxy.keys()).hasSizeGreaterThanOrEqualTo(2)
                    .containsOnly(id.toString());
            assertThat(proxy.bodies()).hasSize(proxy.keys().size())
                    .containsOnly(proxy.bodies().getFirst());
            assertThat(proxy.upstreamStatuses()).hasSizeGreaterThanOrEqualTo(minimumUpstreamCalls).containsOnly(200);
            assertThat(proxy.errors()).isEmpty();
            assertThat(stack.chargeCount(id)).isEqualTo(1);
            assertThat(stack.totalOrderRows()).isEqualTo(1);
            assertThat(stack.stock("ac6-retry")).isEqualTo("0");
            assertThat(records(stack, Topics.ORDERS_CONFIRMED, id)).hasSize(1);
            assertThat(records(stack, Topics.TICKETS_RELEASED, id)).isEmpty();
            assertThat(outboxCount(stack, id, Topics.ORDERS_CONFIRMED)).isEqualTo(1);
            assertThat(records(stack, Topics.PAYMENTS_RESULT, id)).singleElement().satisfies(record ->
                    assertThat(EventJson.mapper().readValue(record.value(), PaymentResult.class).status())
                            .isEqualTo(Status.SUCCEEDED));
        }
    }

    // No production random-source hook exists. Both positive outcome assertions remain
    // probabilistic at rate 0.3, even with 1,000 real requests; do not fake provider decisions.
    @Test
    void AC7_ratePointThree1000OrdersSettleStockPlusSoldConservedUniqueOrdersCharges() throws Exception {
        try (var stack = start("ac7-mixture", 1_000, "0.3")) {
            List<UUID> accepted = buyConcurrently(stack, "ac7-mixture", 1_000);

            await().atMost(AC7_DEADLINE).untilAsserted(() -> {
                Map<String, Long> counts = stack.statusCounts("ac7-mixture");
                assertThat(counts).containsOnlyKeys("CONFIRMED", "FAILED");
                assertThat(counts.getOrDefault("CONFIRMED", 0L)).isPositive();
                assertThat(counts.getOrDefault("FAILED", 0L)).isPositive();
                assertThat(counts.values().stream().mapToLong(Long::longValue).sum()).isEqualTo(1_000);
            });
            awaitOutboxPublished(stack);
            stack.awaitQuiescent(SETTLE);
            awaitReleaseConsumed(stack);
            var counts = stack.statusCounts("ac7-mixture");
            List<OrderConfirmed> confirmations = events(stack, Topics.ORDERS_CONFIRMED, OrderConfirmed.class);
            List<TicketsReleased> releases = events(stack, Topics.TICKETS_RELEASED, TicketsReleased.class);
            List<UUID> sold = confirmations.stream().map(OrderConfirmed::orderId).toList();
            List<UUID> failed = releases.stream().map(TicketsReleased::orderId).toList();

            assertThat(accepted).hasSize(1_000).doesNotHaveDuplicates();
            assertThat(stack.totalOrderRows()).isEqualTo(1_000);
            assertThat(sold).hasSize(counts.get("CONFIRMED").intValue()).doesNotHaveDuplicates();
            assertThat(failed).hasSize(counts.get("FAILED").intValue()).doesNotHaveDuplicates();
            assertThat(sold).doesNotContainAnyElementsOf(failed);
            assertThat(java.util.stream.Stream.concat(sold.stream(), failed.stream()).toList())
                    .containsExactlyInAnyOrderElementsOf(accepted);
            assertThat(releases).allSatisfy(release -> {
                assertThat(release.reason()).isEqualTo("PAYMENT_FAILED");
                assertThat(release.quantity()).isEqualTo(1);
            });
            assertThat(Long.parseLong(stack.stock("ac7-mixture")) + sold.size()).isEqualTo(1_000);
            assertThat(chargeCounts(stack, sold)).isNotEmpty().allSatisfy(count -> assertThat(count).isEqualTo(1));
            assertThat(chargeCounts(stack, failed)).isNotEmpty().allSatisfy(count -> assertThat(count).isZero());
            var succeeded = events(stack, Topics.PAYMENTS_RESULT, PaymentResult.class).stream()
                    .filter(payment -> payment.status() == Status.SUCCEEDED).toList();
            assertThat(succeeded).extracting(PaymentResult::orderId)
                    .containsExactlyInAnyOrderElementsOf(sold);
            assertThat(succeeded).extracting(PaymentResult::providerChargeId)
                    .doesNotContainNull().doesNotHaveDuplicates();
        }
    }
}
