package com.ticketrush.common;

import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Retry then dead-letter policy shared by every consumer (DESIGN.md decision log, 2026-10-30).
 * A failing record is retried with exponential backoff, then published to {@code <topic>.DLT} on
 * the same partition, so the records behind it on that partition keep flowing. Every exception is
 * retried, deserialization failures included: that is the approved policy, and Spring treats those
 * as fatal by default. Services pick this up with {@code @Import(KafkaErrorHandling.class)}; Spring
 * Boot wires the bean into every listener container.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandling {

    /**
     * The template must serialize byte[] as is (see {@link EventJson#producerSerializer()}): a record
     * that could not be deserialized is dead-lettered with its original bytes.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaOperations<?, ?> kafka,
            @Value("${ticketrush.kafka.retry.initial-interval-ms:1000}") long initialIntervalMs,
            @Value("${ticketrush.kafka.retry.multiplier:2.0}") double multiplier,
            @Value("${ticketrush.kafka.retry.max-attempts:4}") int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("ticketrush.kafka.retry.max-attempts must be >= 1");
        }
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxAttempts - 1);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        // Explicit resolver: Spring Kafka 4 defaults to a "-dlt" suffix, the approved name is ".DLT".
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> new TopicPartition(record.topic() + Topics.DLT_SUFFIX, record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.setClassifications(Map.of(), true);
        return handler;
    }
}
