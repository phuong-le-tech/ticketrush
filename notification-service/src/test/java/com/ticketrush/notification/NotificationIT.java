package com.ticketrush.notification;

import static org.awaitility.Awaitility.await;

import com.ticketrush.common.EventJson;
import com.ticketrush.common.OrderConfirmed;
import com.ticketrush.common.Topics;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

import static org.assertj.core.api.Assertions.assertThat;

/** Kafka reel: un OrderConfirmed publie sur orders.confirmed doit produire la ligne NOTIFY. */
@SpringBootTest(properties = "spring.kafka.consumer.auto-offset-reset=earliest")
@ExtendWith(OutputCaptureExtension.class)
class NotificationIT {

    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    static {
        KAFKA.start();
        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(Topics.ORDERS_CONFIRMED, Topics.PARTITIONS, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Test
    void orderConfirmedProducesStableNotifyLogLine(CapturedOutput output) throws Exception {
        UUID orderId = UUID.randomUUID();
        OrderConfirmed confirmed = new OrderConfirmed(UUID.randomUUID(), orderId, "concert-1",
                "customer-9", 3, 2500L, "EUR", Instant.now());

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                new StringSerializer(), EventJson.serializer())) {
            producer.send(new ProducerRecord<>(Topics.ORDERS_CONFIRMED, orderId.toString(), confirmed)).get();
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(output.getAll()).contains(
                "NOTIFY orderId=" + orderId + " customerId=customer-9 quantity=3 amount=2500 EUR"));
    }
}
