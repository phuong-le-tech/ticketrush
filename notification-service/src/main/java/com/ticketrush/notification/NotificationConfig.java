package com.ticketrush.notification;

import com.ticketrush.common.EventJson;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
public class NotificationConfig {

    /** Serde partage: le type est resolu depuis l'en-tete logique __TypeId__. */
    @Bean
    DefaultKafkaConsumerFactoryCustomizer eventJsonConsumer() {
        return factory -> {
            @SuppressWarnings("unchecked")
            var f = (DefaultKafkaConsumerFactory<Object, Object>) factory;
            f.setValueDeserializerSupplier(EventJson::deserializer);
        };
    }
}
