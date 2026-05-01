package com.codeicator.infrastructure;

import com.codeicator.messages.Event;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.time.Instant;

/**
 * Interface for storing and retrieving outbox events.
 *
 * Implementation should use your database technology (JPA, MongoDB, etc).
 * All methods should be transactional to ensure consistency.
 *
 * The outbox pattern guarantees exactly-once delivery:
 * 1. Domain state + events saved in ONE transaction
 * 2. OutboxPublisher polls and publishes events
 * 3. After successful publish, marked as published
 * 4. If publish fails, retry up to MAX_RETRIES times
 * 5. After MAX_RETRIES, move to dead letter queue
 *
 * @see OutboxEvent
 * @see OutboxPublisher
 */
@Service
public interface OutboxStore {

    /**
     * Save a single outbox event.
     * Should be used with DataPersistent to save state and events together.
     *
     * @param event the outbox event to save
     */
    void save(OutboxEvent event);

    /**
     * Save multiple outbox events in a single transaction.
     * Called when a single domain change produces multiple events.
     *
     * All events must be persisted or none (all-or-nothing).
     *
     * @param events list of events to save
     */

    void saveAll(List<OutboxEvent> events);

    /**
     * Get unpublished events up to limit.
     * Called by OutboxPublisher polling job.
     *
     * Should order by createdAt ASC to process in order.
     * Should NOT include dead lettered events.
     *
     * @param limit maximum number of events to return
     * @return list of unpublished events, ordered by age
     */
    List<OutboxEvent> getUnpublished(int limit);

    /**
     * Mark event as successfully published.
     * Should be atomic operation.
     *
     * @param eventId the ID of the event
     */
    void markAsPublished(UUID eventId);

    /**
     * Record failure and increment retry count.
     * Should be atomic operation.
     *
     * @param eventId the ID of the event
     * @param reason the error message from failed publish
     */
    void recordFailure(UUID eventId, String reason, Instant nextAttemptAt);

    /**
     * Move event to dead letter queue.
     * Called when retries exceed maximum.
     * Should be atomic operation.
     *
     * @param event the event to move to DLQ
     */
    void moveToDeadLetter(OutboxEvent event);

    /**
     * Get events in dead letter queue.
     * Used for monitoring and manual intervention.
     *
     * @return list of dead lettered events
     */
    List<OutboxEvent> getDeadLetterEvents();

    /**
     * Get a single event by ID.
     * Used for debugging and recovery operations.
     *
     * @param eventId the event ID
     * @return the event if found
     */
    OutboxEvent findById(UUID eventId);

    /**
     * Clean up old published events.
     * Should be called periodically (e.g., daily) to free disk space.
     *
     * Consider keeping published events for:
     * - Audit trail
     * - Debugging
     * - Compliance requirements
     *
     * @param olderThanMillis delete events published before this time
     * @return number of events deleted
     */
    long deletePublishedBefore(long olderThanMillis);
}
