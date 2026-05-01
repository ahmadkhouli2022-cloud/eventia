package com.codeicator.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;

/**
 * Micrometer-backed metrics recorder for outbox publishing.
 */
public class MicrometerOutboxMetricsRecorder implements OutboxMetricsRecorder {
    private final Counter publishSuccess;
    private final Counter publishFailure;
    private final Counter deadLettered;

    public MicrometerOutboxMetricsRecorder(MeterRegistry registry) {
        this.publishSuccess = registry.counter("eventia.outbox.publish.success");
        this.publishFailure = registry.counter("eventia.outbox.publish.failure");
        this.deadLettered = registry.counter("eventia.outbox.dead_letter");
    }

    @Override
    public void recordPublishSuccess() {
        publishSuccess.increment();
    }

    @Override
    public void recordPublishFailure() {
        publishFailure.increment();
    }

    @Override
    public void recordDeadLetter() {
        deadLettered.increment();
    }
}

