package com.ticketrush.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TopicsTest {

    @Test
    void topicNamesAndPartitionCountMatchDesignDocument() {
        assertThat(List.of(Topics.ORDERS_REQUESTED, Topics.TICKETS_RESERVED, Topics.ORDERS_REJECTED,
                Topics.PAYMENTS_RESULT, Topics.ORDERS_CONFIRMED, Topics.TICKETS_RELEASED))
                .containsExactly("orders.requested", "tickets.reserved", "orders.rejected",
                        "payments.result", "orders.confirmed", "tickets.released");
        assertThat(Topics.PARTITIONS).isEqualTo(12);
    }
}
