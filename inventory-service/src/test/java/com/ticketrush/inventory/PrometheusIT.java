package com.ticketrush.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Idle scrape contract; after-traffic processing deltas are covered by MetricsE2ETest. */
@Tag("m5-red")
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
        properties = "m5.scrape.secret=m5-secret-must-not-be-exported")
class PrometheusIT {
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:8.10.2").withExposedPorts(6379);
    static {
        KAFKA.start();
        REDIS.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @LocalServerPort
    int port;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Test
    void AC2_scrapesBeforeTraffic() throws Exception {
        assertThat(get("/actuator/health/liveness").statusCode()).isEqualTo(200);
        HttpResponse<String> response = get("/actuator/prometheus");
        assertThat(response.statusCode()).isEqualTo(200);
        String body = response.body();
        for (String metric : new String[] {"jvm_memory_used_bytes", "process_uptime_seconds",
                "http_server_requests_seconds_count", "ticketrush_consumer_processing_seconds_count",
                "ticketrush_consumer_processing_seconds_sum"}) {
            assertThat(body).as("sample for %s at idle", metric)
                    .containsPattern("(?m)^" + metric + "(?:\\{[^\\n]*\\})? [0-9].*$");
        }
        assertThat(body).doesNotContain("m5-secret-must-not-be-exported");
        assertThat(body).doesNotContainPattern(
                "(?i)(?:\\{|,)\\s*(?:order_?id|customer_?id|event_?id|message_?id|idempotency_?key|password|secret|token|payload|exception_?message|provider_?charge_?id)=");
        assertThat(get("/actuator/env").statusCode()).isEqualTo(404);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
