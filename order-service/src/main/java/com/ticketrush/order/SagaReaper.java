package com.ticketrush.order;

import java.time.Clock;

/** Compile-only M4 contract. No implementation or Spring wiring. */
public class SagaReaper {
    public SagaReaper(OrderRepository repository, Clock clock) {
        throw new UnsupportedOperationException("M4 saga reaper is not implemented");
    }

    public void reapExpired() {
        throw new UnsupportedOperationException("M4 saga reaper is not implemented");
    }
}
