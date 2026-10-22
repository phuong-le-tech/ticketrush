package com.ticketrush.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import com.ticketrush.common.EventJson;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

/** Milestone 2: orderId derived from (customerId, Idempotency-Key) as a UUIDv5. Kafka is mocked. */
@SpringBootTest
@AutoConfigureMockMvc
class OrderIdempotencyTest {

    private static final UUID NAMESPACE = UUID.fromString("3fc18c52-d4b9-4f65-81fe-4ce095a8f1cc");

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeEach
    void kafkaAcknowledges() {
        // The Spring context calls the mock at startup (preInstantiateSingletons): ignore it.
        clearInvocations(kafkaTemplate);
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    private static String body(String eventId, String customerId, int quantity) {
        return "{\"eventId\":\"" + eventId + "\",\"customerId\":\"" + customerId + "\",\"quantity\":" + quantity + "}";
    }

    private MvcResult postOrder(String key, String body) throws Exception {
        return mockMvc.perform(post("/orders")
                .header("Idempotency-Key", key)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body)).andReturn();
    }

    private static UUID orderIdOf(MvcResult result) throws Exception {
        return UUID.fromString(EventJson.mapper().readTree(result.getResponse().getContentAsString())
                .get("orderId").asString());
    }

    /** Independent UUIDv5 (SHA-1) so the test pins the exact name format of the approved decision. */
    private static UUID expectedUuidV5(String customerId, String key) throws Exception {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        sha1.update(toBytes(NAMESPACE));
        sha1.update((customerId.length() + ":" + customerId + ":" + key).getBytes(StandardCharsets.UTF_8));
        byte[] hash = sha1.digest();
        hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (hash[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (hash[i] & 0xff);
        }
        return new UUID(msb, lsb);
    }

    private static byte[] toBytes(UUID uuid) {
        byte[] out = new byte[16];
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (msb >>> (8 * (7 - i)));
            out[8 + i] = (byte) (lsb >>> (8 * (7 - i)));
        }
        return out;
    }

    @Test
    void sameCustomerAndKeyAC5GiveSameOrderId() throws Exception {
        MvcResult first = postOrder("key-same", body("concert-1", "cust-1", 1));
        MvcResult second = postOrder("key-same", body("concert-1", "cust-1", 1));

        assertThat(first.getResponse().getStatus()).isEqualTo(202);
        assertThat(second.getResponse().getStatus()).isEqualTo(202);
        assertThat(orderIdOf(second)).isEqualTo(orderIdOf(first));
        assertThat(orderIdOf(first)).isEqualTo(expectedUuidV5("cust-1", "key-same"));
        assertThat(orderIdOf(first).version()).isEqualTo(5);
    }

    @Test
    void differentCustomerAC5GivesDifferentOrderId() throws Exception {
        UUID base = orderIdOf(postOrder("key-1", body("concert-1", "cust-1", 1)));

        UUID otherCustomer = orderIdOf(postOrder("key-1", body("concert-1", "cust-2", 1)));

        assertThat(otherCustomer).isNotEqualTo(base);
    }

    @Test
    void differentKeyAC5GivesDifferentOrderId() throws Exception {
        UUID base = orderIdOf(postOrder("key-1", body("concert-1", "cust-1", 1)));

        UUID otherKey = orderIdOf(postOrder("key-2", body("concert-1", "cust-1", 1)));

        assertThat(otherKey).isNotEqualTo(base);
    }

    @Test
    void customerAndKeyBoundaryIsUnambiguousAC5() throws Exception {
        UUID first = orderIdOf(postOrder("c", body("concert-1", "a:b", 1)));

        UUID second = orderIdOf(postOrder("b:c", body("concert-1", "a", 1)));

        assertThat(second).isNotEqualTo(first);
    }

    @Test
    void keyOf255CharactersIsAcceptedAC5() throws Exception {
        MvcResult result = postOrder("k".repeat(255), body("concert-1", "cust-1", 1));

        assertThat(result.getResponse().getStatus()).isEqualTo(202);
    }

    @Test
    void keyOf256CharactersIsRejectedWith400AndNothingPublishedAC5() throws Exception {
        MvcResult result = postOrder("k".repeat(256), body("concert-1", "cust-1", 1));

        assertThat(result.getResponse().getStatus()).isEqualTo(400);
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void blankKeyIsRejectedWith400AC5() throws Exception {
        MvcResult result = postOrder("   ", body("concert-1", "cust-1", 1));

        assertThat(result.getResponse().getStatus()).isEqualTo(400);
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void differentBodySameKeyAC4ReturnsFirstOrderId() throws Exception {
        UUID first = orderIdOf(postOrder("key-body", body("concert-1", "cust-1", 1)));

        MvcResult retry = postOrder("key-body", body("concert-2", "cust-1", 3));

        assertThat(retry.getResponse().getStatus()).isEqualTo(202);
        assertThat(orderIdOf(retry)).isEqualTo(first);
    }

    @Test
    void retryAfter503AC1KeepsOrderId() throws Exception {
        CompletableFuture<org.springframework.kafka.support.SendResult<String, Object>> failed =
                CompletableFuture.failedFuture(new IllegalStateException("broker down"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failed);
        MvcResult unavailable = postOrder("key-retry", body("concert-1", "cust-1", 1));
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        MvcResult retry = postOrder("key-retry", body("concert-1", "cust-1", 1));

        assertThat(unavailable.getResponse().getStatus()).isEqualTo(503);
        assertThat(retry.getResponse().getStatus()).isEqualTo(202);
        assertThat(orderIdOf(retry)).isEqualTo(expectedUuidV5("cust-1", "key-retry"));
    }
}
