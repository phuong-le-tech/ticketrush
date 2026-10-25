package com.ticketrush.gateway;

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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * M3 AC2: Kubernetes readiness and liveness probes need the Actuator health groups on the
 * application port. Real HTTP on a random port; Kafka is a closed port (the probes must not need it).
 * Contract (code-writer): spring-boot-starter-actuator on the classpath and
 * management.endpoint.health.probes.enabled=true, so that GET /actuator/health/readiness and
 * GET /actuator/health/liveness answer 200 {"status":"UP"}.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.bootstrap-servers=localhost:1")
class ActuatorProbesIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();

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
