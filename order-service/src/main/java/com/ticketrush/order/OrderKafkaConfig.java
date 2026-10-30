package com.ticketrush.order;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.KafkaErrorHandling;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/** Serde partage (en-tete __TypeId__ logique) pour que tous les services parlent le meme JSON. */
@Configuration
@Import(KafkaErrorHandling.class)
public class OrderKafkaConfig {

    @Bean
    DefaultKafkaConsumerFactoryCustomizer eventJsonConsumer() {
        return factory -> {
            @SuppressWarnings("unchecked")
            var f = (org.springframework.kafka.core.DefaultKafkaConsumerFactory<Object, Object>) factory;
            f.setValueDeserializerSupplier(EventJson::listenerDeserializer);
        };
    }

    @Bean
    DefaultKafkaProducerFactoryCustomizer eventJsonProducer() {
        return factory -> {
            @SuppressWarnings("unchecked")
            var f = (org.springframework.kafka.core.DefaultKafkaProducerFactory<Object, Object>) factory;
            f.setValueSerializerSupplier(EventJson::producerSerializer);
        };
    }
}
