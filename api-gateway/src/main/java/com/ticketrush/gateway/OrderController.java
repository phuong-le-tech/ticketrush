package com.ticketrush.gateway;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * POST /orders. Cette etape ne fait que la validation (400). La publication Kafka et les reponses
 * 202/503 sont branchees par la tache suivante; en attendant, une requete valide recoit 501.
 */
@RestController
public class OrderController {

    private final OrderRequestValidator validator;

    OrderController(OrderRequestValidator validator) {
        this.validator = validator;
    }

    @PostMapping("/orders")
    ResponseEntity<Void> create(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody CreateOrderRequest request) {
        validator.validate(idempotencyKey, request);
        return ResponseEntity.status(501).build();
    }
}
