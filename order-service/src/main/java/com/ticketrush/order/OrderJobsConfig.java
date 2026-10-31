package com.ticketrush.order;

import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Background jobs of the saga: the outbox relay and the timeout reaper, scheduled with a fixed
 * delay (see their @Scheduled methods). Defaults live in code because the in-process e2e boot
 * skips application.yml.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
class OrderJobsConfig {

    /** Can be switched off, e.g. by tests that drive relays by hand around a forced failure. */
    @Bean
    @ConditionalOnProperty(name = "order.outbox.relay-enabled", havingValue = "true", matchIfMissing = true)
    OutboxRelay outboxRelay(JdbcClient jdbc, KafkaTemplate<String, Object> kafka,
                            PlatformTransactionManager transactionManager) {
        return new OutboxRelay(jdbc, kafka, new TransactionTemplate(transactionManager));
    }

    /** Can be switched off, e.g. by tests that drive the reaper with a fixed clock. */
    @Bean
    @ConditionalOnProperty(name = "order.saga.reaper-enabled", havingValue = "true", matchIfMissing = true)
    SagaReaper sagaReaper(OrderRepository repository,
                          @Value("${order.saga.timeout-seconds:120}") long timeoutSeconds) {
        return new SagaReaper(repository, Clock.systemUTC(), Duration.ofSeconds(timeoutSeconds));
    }
}
