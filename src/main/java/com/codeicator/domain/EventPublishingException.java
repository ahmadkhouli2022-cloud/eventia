package com.codeicator.domain;

/**
 * Exception thrown when event publishing fails in the aggregate.
 *
 * When this exception is thrown:
 * 1. Domain state has been PERSISTED successfully
 * 2. Event publishing FAILED
 * 3. Events are re-queued for retry
 *
 * This is a temporary failure condition. The system should:
 * - Log the error
 * - Retry publishing (already handled by aggregate)
 * - Alert operations if retries continue to fail
 * - Check external message broker connectivity
 *
 * IMPORTANT: This is NOT a domain validation exception.
 * Domain state is safe; only message delivery failed.
 */
public class EventPublishingException extends RuntimeException {

    private final int retryCount;
    private final long timestamp;

    public EventPublishingException(String message) {
        this(message, null, 0);
    }

    public EventPublishingException(String message, Throwable cause) {
        this(message, cause, 0);
    }

    public EventPublishingException(String message, Throwable cause, int retryCount) {
        super(message, cause);
        this.retryCount = retryCount;
        this.timestamp = System.currentTimeMillis();
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Check if this is a temporary/transient failure.
     * Transient failures are likely to succeed on retry.
     */
    public boolean isTransient() {
        Throwable cause = getCause();
        if (cause == null) return true;

        String message = cause.getMessage();
        if (message == null) return true;

        String lowerMessage = message.toLowerCase();
        return lowerMessage.contains("timeout")
            || lowerMessage.contains("connection refused")
            || lowerMessage.contains("temporarily unavailable")
            || lowerMessage.contains("unavailable");
    }
}

