package com.ticketrush.inventory;

import com.ticketrush.common.EventJson;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class InventoryConfig {

    /** 7 days. Also the fallback for contexts that skip application.yml (the in-process e2e boot). */
    private static final String DEFAULT_DEDUP_TTL_SECONDS = "604800";

    @Bean
    InventoryRepository inventoryRepository(StringRedisTemplate redis,
            @Value("${inventory.dedup-ttl-seconds:" + DEFAULT_DEDUP_TTL_SECONDS + "}") long dedupTtlSeconds) {
        return new InventoryRepository(redis, Duration.ofSeconds(dedupTtlSeconds));
    }

    /** Serde partage (en-tete __TypeId__ logique) pour que tous les services parlent le meme JSON. */
    @Bean
    DefaultKafkaConsumerFactoryCustomizer eventJsonConsumer() {
        return factory -> {
            @SuppressWarnings("unchecked")
            var f = (org.springframework.kafka.core.DefaultKafkaConsumerFactory<Object, Object>) factory;
            f.setValueDeserializerSupplier(EventJson::deserializer);
        };
    }

    @Bean
    DefaultKafkaProducerFactoryCustomizer eventJsonProducer() {
        return factory -> {
            @SuppressWarnings("unchecked")
            var f = (org.springframework.kafka.core.DefaultKafkaProducerFactory<Object, Object>) factory;
            f.setValueSerializerSupplier(EventJson::serializer);
        };
    }

    /** Seed au boot, avant le demarrage des listeners (les conteneurs demarrent apres les singletons). */
    @Configuration
    static class Seeder {

        private final InventoryRepository repository;
        private final String eventId;
        private final long stock;
        private final long priceCents;
        private final String currency;

        Seeder(InventoryRepository repository,
               @Value("${inventory.seed.event-id}") String eventId,
               @Value("${inventory.seed.stock}") long stock,
               @Value("${inventory.seed.price-cents}") long priceCents,
               @Value("${inventory.seed.currency}") String currency) {
            this.repository = repository;
            this.eventId = eventId;
            this.stock = stock;
            this.priceCents = priceCents;
            this.currency = currency;
        }

        @PostConstruct
        void seed() {
            repository.seedIfAbsent(eventId, stock, priceCents, currency);
        }
    }
}
