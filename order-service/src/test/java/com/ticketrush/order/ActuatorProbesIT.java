package com.ticketrush.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * M3 AC2: Kubernetes readiness and liveness probes need the Actuator health groups on the
 * application port. Real HTTP on a random port with the shared real Postgres and Kafka (OrderTestInfra).
 * Contract (code-writer): order-service has no web server today (spring-boot-starter only), so it
 * needs spring-boot-starter-webmvc besides spring-boot-starter-actuator, server.port (8084 already set)
 * and management.endpoint.health.probes.enabled=true, so that GET /actuator/health/readiness and
 * GET /actuator/health/liveness answer 200 {"status":"UP"}.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
class ActuatorProbesIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        OrderTestInfra.registerProperties(registry);
    }

    @LocalServerPort
    int port;

    private final HttpClient http = HttpClient.newHttpClient();

    @ParameterizedTest
    @ValueSource(strings = {"/actuator/health/readiness", "/actuator/health/liveness"})
    void AC2_probeEndpointAnswers200WithStatusUp(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).GET().build();

        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode json = JSON.readTree(response.body());
        assertThat(json.get("status").asString()).isEqualTo("UP");
    }
}
