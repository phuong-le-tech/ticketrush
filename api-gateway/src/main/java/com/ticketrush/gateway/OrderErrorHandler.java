package com.ticketrush.gateway;

import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Traduit toute requete invalide en 400 (validation metier, JSON illisible, quantity non entiere). */
@RestControllerAdvice
class OrderErrorHandler {

    @ExceptionHandler(InvalidOrderRequestException.class)
    ResponseEntity<Map<String, String>> invalid(InvalidOrderRequestException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<Map<String, String>> unreadable(HttpMessageNotReadableException e) {
        return ResponseEntity.badRequest().body(Map.of("error", "malformed request body"));
    }
}
