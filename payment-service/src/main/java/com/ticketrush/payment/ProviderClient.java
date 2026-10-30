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
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;

/**
 * Client HTTP du provider. La cle d'idempotence est toujours l'orderId.
 * Only a clear answer is final: a charge (200 SUCCEEDED) or a decline (200 FAILED, or a 4xx that
 * a retry would not change). Anything else (5xx, timeout, network error, unreadable reply) may
 * hide a charge that did happen, so it throws ProviderUnavailableException and the Kafka error
 * handler retries with the same key: the provider then returns the stored charge, never a second one.
 */
@Component
public class ProviderClient {

    private static final int HTTP_OK = 200;
    private static final int HTTP_CLIENT_ERROR = 400;
    private static final int HTTP_SERVER_ERROR = 500;

    /** Resultat d'un appel: chargeId si succes, sinon failureReason. */
    record Outcome(String chargeId, String failureReason) {
        boolean succeeded() {
            return chargeId != null;
        }
    }

    /** The provider gave no usable answer; the charge may or may not exist. Safe to retry. */
    static final class ProviderUnavailableException extends RuntimeException {
        ProviderUnavailableException(String message, Throwable cause) {
            super(message, cause);
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

    Outcome charge(UUID orderId, long amountCents, String currency) throws InterruptedException {
        String body = EventJson.mapper().writeValueAsString(
                Map.of("orderId", orderId.toString(), "amountCents", amountCents, "currency", currency));
        HttpRequest request = HttpRequest.newBuilder(chargesUri)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", orderId.toString())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            // Inclut HttpTimeoutException (connexion ou lecture) et une reponse coupee.
            throw new ProviderUnavailableException("provider call failed: " + e.getClass().getSimpleName(), e);
        }
        int status = response.statusCode();
        if (status >= HTTP_CLIENT_ERROR && status < HTTP_SERVER_ERROR) {
            return new Outcome(null, "PROVIDER_HTTP_" + status);
        }
        if (status != HTTP_OK) {
            throw new ProviderUnavailableException("provider answered HTTP " + status, null);
        }
        return outcome(response.body());
    }

    private static Outcome outcome(String responseBody) {
        JsonNode json;
        try {
            json = EventJson.mapper().readTree(responseBody);
        } catch (JacksonException e) {
            throw new ProviderUnavailableException("unreadable provider reply", e);
        }
        String status = json.path("status").asString("");
        String chargeId = json.path("chargeId").asString("");
        if ("SUCCEEDED".equals(status) && !chargeId.isBlank()) {
            return new Outcome(chargeId, null);
        }
        if ("FAILED".equals(status)) {
            return new Outcome(null, "PROVIDER_DECLINED");
        }
        throw new ProviderUnavailableException("unexpected provider reply status '" + status + "'", null);
    }
}
