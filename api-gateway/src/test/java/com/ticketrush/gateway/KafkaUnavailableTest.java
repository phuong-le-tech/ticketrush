package com.ticketrush.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

/** Kafka injoignable (port ferme, vrai producer): 503 et jamais 202, en moins de 30 s. */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=localhost:1",
        "spring.kafka.producer.properties.max.block.ms=2000",
        "spring.kafka.producer.properties.request.timeout.ms=1000",
        "spring.kafka.producer.properties.delivery.timeout.ms=3000"
})
@AutoConfigureMockMvc
class KafkaUnavailableTest {

    @Autowired
    MockMvc mockMvc;

    @Test
    void kafkaDownReturns503AndNeverAccepts() {
        int status = assertTimeoutPreemptively(Duration.ofSeconds(30), () -> mockMvc.perform(post("/orders")
                        .header("Idempotency-Key", "key-down")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":\"concert-1\",\"customerId\":\"c-1\",\"quantity\":2}"))
                .andReturn().getResponse().getStatus());

        assertThat(status).isEqualTo(503);
    }
}
