package com.codeicator.domain;

import com.codeicator.messages.Event;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.UUID;

/**
 * Represents an event pending publishing in the outbox pattern.
 *
 * Stores events in durable storage before they're published.
 * This ensures guaranteed delivery even if the application crashes.
 *
 * Lifecycle:
 * 1. Created when domain event occurs
 * 2. Stored in OUTBOX table along with aggregate state (same transaction)
 * 3. Polled by OutboxPublisher and published to message bus
 * 4. Marked as published when successful
 * 5. Moved to DLQ if retries exceed max
 *
 * @see OutboxStore
 * @see OutboxPublisher
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
public class OutboxEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Unique identifier for this outbox entry
     */
    @Id
    private String id;

    /**
     * The actual domain event to publish
     */
    private Event event;

    /**
     * Timestamp when event was stored in outbox (milliseconds)
     */
    private long createdAt;

    /**
     * Timestamp when event was successfully published (null if not published)
     */
    private Long publishedAt;

    /**
     * Number of times publishing was attempted
     */
    private int retryCount;

    /**
     * Last error message from failed publish attempt
     */
    private String failureReason;

    /**
     * Whether this event has been moved to dead letter queue
     */
    @Builder.Default
    private boolean deadLettered = false;

    /**
     * Factory method to create outbox event from domain event
     * @param event the domain event
     * @return new OutboxEvent ready for storage
     */
    public static OutboxEvent from(Event event) {
        return OutboxEvent.builder()
            .id(UUID.randomUUID().toString())
            .event(event)
            .createdAt(System.currentTimeMillis())
            .retryCount(0)
            .deadLettered(false)
            .build();
    }

    /**
     * Mark this event as successfully published
     */
    public void markPublished() {
        this.publishedAt = System.currentTimeMillis();
    }

    /**
     * Record failure and increment retry count
     * @param reason why the publish failed
     */
    public void recordFailure(String reason) {
        this.failureReason = reason;
        this.retryCount++;
    }

    /**
     * Check if event has been published
     * @return true if publishedAt is not null
     */
    @JsonIgnore
    public boolean isPublished() {
        return publishedAt != null;
    }

    /**
     * Check if event should be retried
     * @param maxRetries maximum number of retry attempts
     * @return true if not published, under max retries, and not dead lettered
     */
    @JsonIgnore
    public boolean shouldRetry(int maxRetries) {
        return !isPublished() && retryCount < maxRetries && !deadLettered;
    }

    /**
     * Get age of this outbox event in milliseconds
     * @return time since created
     */
    @JsonIgnore
    public long getAgeMillis() {
        return System.currentTimeMillis() - createdAt;
    }
}

