package com.ticketrush.notification;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.KafkaErrorHandling;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaConsumerFactoryCustomizer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

@Configuration
@Import(KafkaErrorHandling.class)
public class NotificationConfig {

    /** Serde partage: le type est resolu depuis l'en-tete logique __TypeId__. */
    @Bean
    DefaultKafkaConsumerFactoryCustomizer eventJsonConsumer() {
        return factory -> {
            @SuppressWarnings("unchecked")
            var f = (DefaultKafkaConsumerFactory<Object, Object>) factory;
            f.setValueDeserializerSupplier(EventJson::listenerDeserializer);
        };
    }

    /** Only the dead-letter recoverer produces here; it needs the byte[]-aware serde. */
    @Bean
    DefaultKafkaProducerFactoryCustomizer eventJsonProducer() {
        return factory -> {
            @SuppressWarnings("unchecked")
            var f = (DefaultKafkaProducerFactory<Object, Object>) factory;
            f.setValueSerializerSupplier(EventJson::producerSerializer);
        };
    }
}
