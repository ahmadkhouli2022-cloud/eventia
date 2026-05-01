package com.codeicator.infrastructure;

/**
 * Default metrics recorder that performs no operations.
 */
public class NoopOutboxMetricsRecorder implements OutboxMetricsRecorder {
    @Override
    public void recordPublishSuccess() {
        // No-op
    }

    @Override
    public void recordPublishFailure() {
        // No-op
    }

    @Override
    public void recordDeadLetter() {
        // No-op
    }
}

