package com.ticketrush.order;

import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderActivityController {
    private static final int MAX_LIMIT = 50;
    private final OrderRepository orders;

    OrderActivityController(OrderRepository orders) {
        this.orders = orders;
    }

    @GetMapping("/orders/activity")
    ResponseEntity<Activity> activity(@RequestParam(name = "limit", defaultValue = "20") int limit) {
        if (limit < 1 || limit > MAX_LIMIT) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(new Activity(OrderRepository.ACTIVITY_WINDOW_SECONDS,
                orders.countOrdersInWindow(), orders.recentOrders(limit)));
    }

    record Activity(int windowSeconds, long ordersInWindow, List<OrderRepository.RecentOrder> orders) { }
}
