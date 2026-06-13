package com.codeicator.infrastructure;

import com.codeicator.messages.Event;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.*;
import lombok.*;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "outbox_events")  // ✅ Explicit table name
public class OutboxEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "id")
    private UUID id;

    /**
     * Type of the event (fully qualified class name)
     * Needed for deserialization
     */
    @Column(name = "event_type", nullable = false, length = 500)
    private String eventType;

    /**
     * The serialized event payload (JSON)
     */
    @Column(name = "event_payload", nullable = false, columnDefinition = "TEXT")
    @Lob
    private String eventPayload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    @Lob
    private String failureReason;

    @Column(name = "dead_lettered", nullable = false)
    @Builder.Default
    private boolean deadLettered = false;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "schema_version", nullable = false)
    private int schemaVersion;

    /**
     * Factory method to create outbox event from domain event
     */
    public static OutboxEvent from(Event event, ObjectMapper objectMapper) {
        try {
            return OutboxEvent.builder()
                .id(event.getId())
                .eventType(event.getClass().getName())
                .eventPayload(objectMapper.writeValueAsString(event))
                .schemaVersion(event.getSchemaVersion())
                .createdAt(event.getRaisedAt().toInstant())
                .retryCount(0)
                .deadLettered(false)
                .build();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }

    /**
     * Deserialize the event from JSON
     */
    @JsonIgnore
    public Event getEvent(ObjectMapper objectMapper) {
        try {
            Class<?> eventClass = Class.forName(eventType);
            return (Event) objectMapper.readValue(eventPayload, eventClass);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize event", e);
        }
    }

    public void markPublished() {
        this.publishedAt = Instant.now();
    }

    public void recordFailure(String reason, Instant nextAttemptAt) {
        this.failureReason = reason;
        this.retryCount++;
        this.nextAttemptAt = nextAttemptAt;
    }

    @JsonIgnore
    public boolean isPublished() {
        return publishedAt != null;
    }

    @JsonIgnore
    public boolean shouldRetry(int maxRetries) {
        return !isPublished() && retryCount < maxRetries && !deadLettered;
    }

    @JsonIgnore
    public long getAgeMillis() {
        if (createdAt == null) {
            return 0L;
        }
        return Instant.now().toEpochMilli() - createdAt.toEpochMilli();
    }

    @JsonIgnore
    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }
}
