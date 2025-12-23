package com.codeicator.domain;

import com.codeicator.messages.Event;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.*;
import lombok.*;

import java.io.Serializable;
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
    @Column(name = "id", length = 36)
    private String id;

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
    private long createdAt;

    @Column(name = "published_at")
    private Long publishedAt;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    @Lob
    private String failureReason;

    @Column(name = "dead_lettered", nullable = false)
    @Builder.Default
    private boolean deadLettered = false;

    /**
     * Factory method to create outbox event from domain event
     */
    public static OutboxEvent from(Event event, ObjectMapper objectMapper) {
        try {
            return OutboxEvent.builder()
                .id(UUID.randomUUID().toString())
                .eventType(event.getClass().getName())
                .eventPayload(objectMapper.writeValueAsString(event))
                .createdAt(System.currentTimeMillis())
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
        this.publishedAt = System.currentTimeMillis();
    }

    public void recordFailure(String reason) {
        this.failureReason = reason;
        this.retryCount++;
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
        return System.currentTimeMillis() - createdAt;
    }
}
