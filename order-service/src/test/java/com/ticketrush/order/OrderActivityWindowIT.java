package com.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.JsonNode;

@Tag("m7-red")
class OrderActivityWindowIT extends OrderActivityTestSupport {
    @Autowired OrderRepository repository;

    static Stream<Arguments> cutoffs() {
        return Stream.of(Arguments.of(-1, 0L), Arguments.of(0, 0L), Arguments.of(1, 1L));
    }

    @ParameterizedTest
    @MethodSource("cutoffs")
    @Transactional
    void AC4_strictWindow_repositoryUsesExclusiveCreatedAtCutoff(int microseconds, long expected) {
        // PostgreSQL now() is stable in this transaction, including the real repository call.
        jdbc.update("""
                INSERT INTO orders.orders (order_id, status, created_at, updated_at)
                VALUES ('00000000-0000-0000-0000-000000000004', 'CONFIRMED',
                        now() - interval '10 seconds' + ? * interval '1 microsecond', now())
                """, microseconds);

        assertThatCode(() -> assertThat(repository.countOrdersInWindow()).isEqualTo(expected))
                .as("AC4 real repository must count created_at strictly greater than PostgreSQL now() - 10s")
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @CsvSource({"1,1", "20,6", "50,6"})
    void AC4_strictWindow_httpCountIgnoresLimitStatusAndUpdatedAt(int limit, int expectedRows) throws Exception {
        // Ten seconds of margin outside, eight seconds inside. No wall-clock sleeps or rounding.
        jdbc.update("""
                INSERT INTO orders.orders (order_id, status, created_at, updated_at)
                SELECT md5('m7-window-' || n)::uuid,
                    (ARRAY['PENDING','RESERVED','CONFIRMED','FAILED','REJECTED'])[n],
                    now() - interval '2 seconds', now() - interval '2 seconds'
                FROM generate_series(1, 5) AS n
                """);
        jdbc.update("""
                INSERT INTO orders.orders (order_id, status, created_at, updated_at)
                VALUES ('00000000-0000-0000-0000-000000000005', 'CONFIRMED',
                        now() - interval '20 seconds', now())
                """);

        JsonNode body = assertActivity(get("?limit=" + limit));

        assertThat(body.get("ordersInWindow").asLong()).isEqualTo(5L);
        assertThat(body.get("orders").size()).isEqualTo(expectedRows);
    }

    @Test
    void AC4_strictWindow_emptyHasZeroCount() throws Exception {
        JsonNode body = assertActivity(get(""));

        assertThat(body.get("ordersInWindow").asLong()).isZero();
        assertThat(body.get("orders").size()).isZero();
    }
}
