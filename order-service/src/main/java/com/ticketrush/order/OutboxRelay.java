package com.ticketrush.order;

import com.ticketrush.common.EventJson;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Publishes orders.outbox rows. One transaction per batch: lock the oldest unpublished rows with
 * FOR UPDATE SKIP LOCKED (a second relay takes other rows instead of waiting or double sending),
 * send them, wait for every Kafka ack, then set published_at. Any failure rolls back, so the rows
 * stay unpublished and the next run sends them again: at-least-once, consumers dedupe. A crash
 * after the ack but before the commit re-sends the same payload (same messageId).
 */
public class OutboxRelay {

    static final int BATCH_SIZE = 200;

    private record Row(UUID id, String topic, String key, String type, String payload) {
    }

    private final JdbcClient jdbc;
    private final KafkaTemplate<String, Object> kafka;
    private final TransactionOperations transactions;

    public OutboxRelay(JdbcClient jdbc, KafkaTemplate<String, Object> kafka, TransactionOperations transactions) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.transactions = transactions;
    }

    /** Publishes one batch. Throws if Kafka or the DB fails; nothing is marked in that case. */
    @Scheduled(fixedDelayString = "${order.outbox.relay-interval-ms:100}")
    public void publishPending() {
        transactions.executeWithoutResult(status -> {
            List<Row> rows = jdbc.sql("""
                    SELECT id, topic, msg_key, type, payload::text AS payload FROM orders.outbox
                    WHERE published_at IS NULL
                    ORDER BY created_at, id LIMIT :limit
                    FOR UPDATE SKIP LOCKED""")
                    .param("limit", BATCH_SIZE)
                    .query((rs, n) -> new Row(rs.getObject("id", UUID.class), rs.getString("topic"),
                            rs.getString("msg_key"), rs.getString("type"), rs.getString("payload")))
                    .list();
            if (rows.isEmpty()) {
                return;
            }
            // Send the whole batch, then wait: the producer keeps per-partition order.
            List<CompletableFuture<SendResult<String, Object>>> acks = rows.stream()
                    .map(row -> kafka.send(row.topic(), row.key(),
                            EventJson.mapper().readValue(row.payload(), EventJson.classOf(row.type()))))
                    .toList();
            awaitAll(acks);
            jdbc.sql("UPDATE orders.outbox SET published_at = now() WHERE id IN (:ids)")
                    .param("ids", rows.stream().map(Row::id).toList())
                    .update();
        });
    }

    private static void awaitAll(List<CompletableFuture<SendResult<String, Object>>> acks) {
        try {
            CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("Interrupted while waiting for outbox acks", e);
        } catch (ExecutionException e) {
            throw new KafkaException("Outbox publish failed, rows stay unpublished", e.getCause());
        }
    }
}
