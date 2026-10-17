package com.ticketrush.payment;

import com.ticketrush.common.EventJson;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

/** Client HTTP du provider. La cle d'idempotence est toujours l'orderId. Pas de retry en M1. */
@Component
public class ProviderClient {

    /** Resultat d'un appel: chargeId si succes, sinon failureReason. */
    record Outcome(String chargeId, String failureReason) {
        boolean succeeded() {
            return chargeId != null;
        }
    }

    private final HttpClient http;
    private final URI chargesUri;
    private final Duration timeout;

    ProviderClient(@Value("${payment.provider.url}") String baseUrl,
                   @Value("${payment.provider.timeout-ms}") long timeoutMs) {
        this.timeout = Duration.ofMillis(timeoutMs);
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
        this.chargesUri = URI.create(baseUrl.replaceAll("/+$", "") + "/charges");
    }

    Outcome charge(UUID orderId, long amountCents, String currency) {
        try {
            String body = EventJson.mapper().writeValueAsString(
                    Map.of("orderId", orderId.toString(), "amountCents", amountCents, "currency", currency));
            HttpRequest request = HttpRequest.newBuilder(chargesUri)
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", orderId.toString())
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return new Outcome(null, "PROVIDER_HTTP_" + response.statusCode());
            }
            JsonNode json = EventJson.mapper().readTree(response.body());
            String chargeId = json.path("chargeId").asString();
            if (!"SUCCEEDED".equals(json.path("status").asString()) || chargeId == null || chargeId.isBlank()) {
                return new Outcome(null, "PROVIDER_DECLINED");
            }
            return new Outcome(chargeId, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Outcome(null, "PROVIDER_INTERRUPTED");
        } catch (IOException e) {
            // Inclut HttpTimeoutException (connexion ou lecture) et erreurs reseau.
            return new Outcome(null, "PROVIDER_UNAVAILABLE: " + e.getClass().getSimpleName());
        } catch (RuntimeException e) {
            return new Outcome(null, "PROVIDER_ERROR: " + e.getClass().getSimpleName());
        }
    }
}
