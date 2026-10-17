package com.ticketrush.gateway;

import com.ticketrush.common.EventJson;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

@Configuration
public class GatewayConfig {

    /** Serde partage: l'en-tete __TypeId__ porte le nom logique (OrderRequested). */
    @Bean
    DefaultKafkaProducerFactoryCustomizer eventJsonProducer() {
        return factory -> {
            @SuppressWarnings("unchecked")
            var f = (DefaultKafkaProducerFactory<Object, Object>) factory;
            f.setValueSerializerSupplier(EventJson::serializer);
        };
    }
}
