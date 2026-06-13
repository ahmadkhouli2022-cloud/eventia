package com.codeicator.infrastructure.persistence;

import com.codeicator.infrastructure.OutboxEvent;
import com.codeicator.infrastructure.OutboxStore;
import com.codeicator.messages.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * JPA implementation of OutboxStore.
 *
 * Provides transactional persistence of outbox events using Spring Data JPA.
 * Ensures all operations are atomic and consistent.
 *
 * Thread-safe: All methods are transactional.
 * Transaction boundary: Method-level (Spring @Transactional)
 */
@Service
@Slf4j
public class JpaOutboxStore implements OutboxStore {

    private final OutboxEventRepository repository;

    public JpaOutboxStore(OutboxEventRepository repository) {
        this.repository = Objects.requireNonNull(repository,
            "OutboxEventRepository cannot be null");
    }

    /**
     * Save a single outbox event.
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public void save(OutboxEvent event) {
        Objects.requireNonNull(event, "OutboxEvent cannot be null");
        repository.save(event);
        log.debug("Saved outbox event: {}", event.getId());
    }

    /**
     * Save multiple outbox events in a single transaction.
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public void saveAll(List<OutboxEvent> events) {
        Objects.requireNonNull(events, "Events list cannot be null");

        if (events.isEmpty()) {
            log.debug("No events to save");
            return;
        }

        repository.saveAll(events);
        log.debug("Saved {} outbox events", events.size());
    }

    /**
     * Get unpublished events up to limit.
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<OutboxEvent> getUnpublished(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive");
        }

        List<OutboxEvent> unpublished = repository.findUnpublished(limit, Instant.now());
        log.debug("Found {} unpublished events", unpublished.size());
        return unpublished;
    }

    /**
     * Mark event as successfully published.
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public void markAsPublished(UUID eventId) {
        Objects.requireNonNull(eventId, "Event ID cannot be null");

        repository.markAsPublished(eventId, Instant.now());
        log.debug("Marked event as published: {}", eventId);
    }

    /**
     * Record failure and increment retry count.
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public void recordFailure(UUID eventId, String reason, Instant nextAttemptAt) {
        Objects.requireNonNull(eventId, "Event ID cannot be null");
        Objects.requireNonNull(reason, "Failure reason cannot be null");
        Objects.requireNonNull(nextAttemptAt, "Next attempt time cannot be null");

        try {
            repository.recordFailure(eventId, reason, nextAttemptAt);
            log.debug("Recorded failure for event {}: {}", eventId, reason);
        } catch (Exception e) {
            log.error("Failed to record failure for event {}", eventId, e);
            throw e;  // Re-throw to signal caller
        }
    }

    /**
     * Move event to dead letter queue.
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public void moveToDeadLetter(OutboxEvent event) {
        Objects.requireNonNull(event, "OutboxEvent cannot be null");
        Objects.requireNonNull(event.getId(), "Event ID cannot be null");

        event.setDeadLettered(true);
        repository.save(event);
        log.error("Moved event to dead letter queue: {}", event.getId());
    }

    /**
     * Get events in dead letter queue.
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public List<OutboxEvent> getDeadLetterEvents() {
        List<OutboxEvent> deadLettered = repository.findDeadLettered();
        log.debug("Found {} dead-lettered events", deadLettered.size());
        return deadLettered;
    }

    @Override
    @Transactional(readOnly = true)
    public List<OutboxEvent> getDeadLetterEvents(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive");
        }

        List<OutboxEvent> deadLettered = repository.findDeadLettered(limit);
        log.debug("Found {} dead-lettered events", deadLettered.size());
        return deadLettered;
    }

    @Override
    @Transactional(readOnly = true)
    public List<OutboxEvent> getDeadLetterEventsBetween(Instant from, Instant to, int limit) {
        Objects.requireNonNull(from, "From timestamp cannot be null");
        Objects.requireNonNull(to, "To timestamp cannot be null");
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive");
        }

        List<OutboxEvent> deadLettered = repository.findDeadLetteredBetween(from, to, limit);
        log.debug("Found {} dead-lettered events between {} and {}", deadLettered.size(), from, to);
        return deadLettered;
    }

    @Override
    @Transactional(readOnly = true)
    public List<OutboxEvent> getDeadLetterEventsByIds(List<UUID> ids) {
        Objects.requireNonNull(ids, "Ids cannot be null");
        if (ids.isEmpty()) {
            return List.of();
        }

        List<OutboxEvent> deadLettered = repository.findDeadLetteredByIds(ids);
        log.debug("Found {} dead-lettered events by ids", deadLettered.size());
        return deadLettered;
    }

    /**
     * Get a single event by ID.
     * {@inheritDoc}
     */
    @Override
    @Transactional(readOnly = true)
    public OutboxEvent findById(UUID eventId) {
        Objects.requireNonNull(eventId, "Event ID cannot be null");
        return repository.findById(eventId).orElse(null);
    }

    /**
     * Clean up old published events.
     * {@inheritDoc}
     */
    @Override
    @Transactional
    public long deletePublishedBefore(long olderThanMillis) {
        if (olderThanMillis <= 0) {
            throw new IllegalArgumentException("Timestamp must be positive");
        }

        Instant threshold = Instant.ofEpochMilli(olderThanMillis);
        long deleted = repository.deletePublishedBefore(threshold);
        log.info("Deleted {} old published events", deleted);
        return deleted;
    }

    /**
     * Get count of unpublished events (for monitoring).
     * More efficient than fetching all events.
     *
     * @return count of unpublished events
     */
    public long countUnpublished() {
        return repository.countUnpublished();
    }

    /**
     * Get count of dead-lettered events (for monitoring).
     *
     * @return count of dead-lettered events
     */
    public long countDeadLettered() {
        return repository.countDeadLettered();
    }
}
