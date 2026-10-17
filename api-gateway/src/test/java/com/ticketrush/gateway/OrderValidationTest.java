package com.ticketrush.gateway;

import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

/** Validation 400: aucune publication Kafka. Kafka est mocke car il n'est pas le sujet ici. */
@SpringBootTest
@AutoConfigureMockMvc
class OrderValidationTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    KafkaTemplate<String, Object> kafkaTemplate;

    static Stream<Arguments> invalidRequests() {
        return Stream.of(
                Arguments.of("quantity 0", "key-1",
                        "{\"eventId\":\"concert-1\",\"customerId\":\"c-1\",\"quantity\":0}"),
                Arguments.of("quantity 5", "key-1",
                        "{\"eventId\":\"concert-1\",\"customerId\":\"c-1\",\"quantity\":5}"),
                Arguments.of("quantity absente", "key-1",
                        "{\"eventId\":\"concert-1\",\"customerId\":\"c-1\"}"),
                Arguments.of("header Idempotency-Key absent", null,
                        "{\"eventId\":\"concert-1\",\"customerId\":\"c-1\",\"quantity\":2}"),
                Arguments.of("eventId vide", "key-1",
                        "{\"eventId\":\"\",\"customerId\":\"c-1\",\"quantity\":2}"),
                Arguments.of("customerId vide", "key-1",
                        "{\"eventId\":\"concert-1\",\"customerId\":\"\",\"quantity\":2}"));
    }

    @ParameterizedTest(name = "{0} -> 400")
    @MethodSource("invalidRequests")
    void invalidRequestIsRejectedWith400AndNothingIsPublished(String label, String idempotencyKey, String body)
            throws Exception {
        MockHttpServletRequestBuilder request = post("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body);
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }

        mockMvc.perform(request).andExpect(status().isBadRequest());

        verifyNoInteractions(kafkaTemplate);
    }
}
