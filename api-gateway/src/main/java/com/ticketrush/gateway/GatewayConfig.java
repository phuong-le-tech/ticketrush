package com.ticketrush.gateway;

import com.ticketrush.common.EventJson;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

@Configuration
public class GatewayConfig {

    /** Serde partage: en-tete __TypeId__ logique (ex. OrderRequested), lu par tous les services. */
    @Bean
    DefaultKafkaProducerFactoryCustomizer eventJsonProducer() {
        return factory -> {
            @SuppressWarnings("unchecked")
            var f = (DefaultKafkaProducerFactory<Object, Object>) factory;
            f.setValueSerializerSupplier(EventJson::serializer);
        };
    }
}
