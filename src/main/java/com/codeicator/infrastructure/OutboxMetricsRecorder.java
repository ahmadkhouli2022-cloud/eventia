package com.codeicator.infrastructure;

/**
 * Optional hooks for capturing outbox publish metrics.
 */
public interface OutboxMetricsRecorder {
    void recordPublishSuccess();

    void recordPublishFailure();

    void recordDeadLetter();
}

