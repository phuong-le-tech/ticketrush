package com.ticketrush.inventory;

import com.ticketrush.common.TicketsReleased;
import com.ticketrush.common.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes tickets.released (payment failure or saga timeout, from order-service) and gives the
 * order's tickets back. release.lua dedupes by orderId, so a replayed or re-published release
 * never restocks twice.
 */
@Component
public class TicketsReleasedListener {

    private static final Logger log = LoggerFactory.getLogger(TicketsReleasedListener.class);

    private final InventoryRepository repository;
    private Timer processing;

    @Autowired
    void registerMetrics(MeterRegistry registry) {
        processing = registry.timer("ticketrush.consumer.processing", "consumer", "inventory-service",
                "topic", Topics.TICKETS_RELEASED);
    }

    TicketsReleasedListener(InventoryRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = Topics.TICKETS_RELEASED)
    void onTicketsReleased(TicketsReleased release, Acknowledgment ack) {
        long started = System.nanoTime();
        try {
            long restocked = repository.release(release.eventId(), release.orderId());
            if (restocked > 0) {
                log.info("Released {} tickets of order {} for event {} ({})", restocked, release.orderId(),
                        release.eventId(), release.reason());
            } else {
                log.debug("Release of order {} ignored: no live reservation", release.orderId());
            }
            ack.acknowledge();
        } finally {
            if (processing != null) processing.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
        }
    }
}
