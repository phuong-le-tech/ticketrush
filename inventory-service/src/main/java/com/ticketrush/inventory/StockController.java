package com.ticketrush.inventory;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StockController {
    private static final int MAX_EVENT_ID_LENGTH = 255;
    private final InventoryRepository inventory;

    StockController(InventoryRepository inventory) {
        this.inventory = inventory;
    }

    @GetMapping("/events/{eventId}/stock")
    ResponseEntity<Stock> stock(@PathVariable("eventId") String eventId) {
        if (eventId.isBlank() || eventId.length() > MAX_EVENT_ID_LENGTH) {
            return ResponseEntity.badRequest().build();
        }
        return inventory.stockOf(eventId)
                .map(stock -> ResponseEntity.ok(new Stock(eventId, stock)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    record Stock(String eventId, long stock) { }
}
