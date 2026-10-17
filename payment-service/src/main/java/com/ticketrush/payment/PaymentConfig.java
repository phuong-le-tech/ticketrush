package com.ticketrush.payment;

import com.ticketrush.common.EventJson;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PaymentConfig {

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
}
