package com.codeicator.infrastructure.persistence;

import com.codeicator.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * JPA Repository for OutboxEvent persistence.
 *
 * Handles all database operations for outbox events.
 * All methods are transactional to ensure data consistency.
 */
@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Find all unpublished outbox events ordered by creation time.
     * Does not include dead-lettered events.
     *
     * @param limit maximum number of events to return
     * @return list of unpublished events in creation order
     */
    @Query(value = """
        SELECT o FROM OutboxEvent o
        WHERE o.publishedAt IS NULL AND o.deadLettered = FALSE
        ORDER BY o.createdAt ASC
        LIMIT :limit
        """)
    List<OutboxEvent> findUnpublished(int limit);

    /**
     * Mark a single event as published.
     * Atomic operation - either succeeds or fails completely.
     *
     * @param id the event ID
     * @param publishedAt timestamp of publication
     */
    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent o SET o.publishedAt = :publishedAt WHERE o.id = :id")
    void markAsPublished(String id, long publishedAt);

    /**
     * Record failure for an event and increment retry count.
     * Atomic operation.
     *
     * @param id the event ID
     * @param reason the failure reason
     */
    @Modifying
    @Transactional
    @Query("""
        UPDATE OutboxEvent o
        SET o.retryCount = o.retryCount + 1,
            o.failureReason = :reason
        WHERE o.id = :id
        """)
    void recordFailure(String id, String reason);

    /**
     * Get all dead-lettered events.
     * Used for monitoring and manual intervention.
     *
     * @return list of dead-lettered events
     */
    @Query("SELECT o FROM OutboxEvent o WHERE o.deadLettered = TRUE")
    List<OutboxEvent> findDeadLettered();

    /**
     * Count unpublished events (for monitoring).
     * More efficient than fetching all.
     *
     * @return count of unpublished events
     */
    @Query("SELECT COUNT(o) FROM OutboxEvent o " +
           "WHERE o.publishedAt IS NULL AND o.deadLettered = FALSE")
    long countUnpublished();

    /**
     * Count dead-lettered events (for monitoring).
     *
     * @return count of dead-lettered events
     */
    @Query("SELECT COUNT(o) FROM OutboxEvent o WHERE o.deadLettered = TRUE")
    long countDeadLettered();

    /**
     * Delete published events older than specified timestamp.
     * Used for cleanup and archival.
     *
     * @param olderThanMillis delete events published before this time
     * @return number of events deleted
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM OutboxEvent o WHERE o.publishedAt < :olderThanMillis")
    long deletePublishedBefore(long olderThanMillis);

    /**
     * Mark an event as dead-lettered.
     * After max retries exceeded.
     *
     * @param id the event ID
     */
    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent o SET o.deadLettered = TRUE WHERE o.id = :id")
    void markAsDeadLettered(String id);
}

