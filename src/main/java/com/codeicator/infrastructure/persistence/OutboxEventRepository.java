package com.codeicator.infrastructure.persistence;

import com.codeicator.infrastructure.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * JPA Repository for OutboxEvent persistence.
 *
 * Handles all database operations for outbox events.
 * All methods are transactional to ensure data consistency.
 */
@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Find all unpublished outbox events ordered by creation time.
     * Does not include dead-lettered events.
     *
     * @param limit maximum number of events to return
     * @return list of unpublished events in creation order
     */
    @Query(value = """
        SELECT * FROM outbox_events
        WHERE published_at IS NULL
          AND dead_lettered = FALSE
          AND (next_attempt_at IS NULL OR next_attempt_at <= :now)
        ORDER BY created_at ASC
        LIMIT :limit
        """, nativeQuery = true)
    List<OutboxEvent> findUnpublished(@Param("limit") int limit, @Param("now") Instant now);

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
    void markAsPublished(@Param("id") UUID id, @Param("publishedAt") Instant publishedAt);

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
            o.failureReason = :reason,
            o.nextAttemptAt = :nextAttemptAt
        WHERE o.id = :id
        """)
    void recordFailure(
        @Param("id") UUID id,
        @Param("reason") String reason,
        @Param("nextAttemptAt") Instant nextAttemptAt);

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
     * @param olderThan delete events published before this time
     * @return number of events deleted
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM OutboxEvent o WHERE o.publishedAt < :olderThan")
    long deletePublishedBefore(@Param("olderThan") Instant olderThan);

    /**
     * Mark an event as dead-lettered.
     * After max retries exceeded.
     *
     * @param id the event ID
     */
    @Modifying
    @Transactional
    @Query("UPDATE OutboxEvent o SET o.deadLettered = TRUE WHERE o.id = :id")
    void markAsDeadLettered(@Param("id") UUID id);

    @Query(value = """
        SELECT * FROM outbox_events
        WHERE dead_lettered = TRUE
        ORDER BY created_at ASC
        LIMIT :limit
        """, nativeQuery = true)
    List<OutboxEvent> findDeadLettered(@Param("limit") int limit);

    @Query(value = """
        SELECT * FROM outbox_events
        WHERE dead_lettered = TRUE
          AND created_at BETWEEN :from AND :to
        ORDER BY created_at ASC
        LIMIT :limit
        """, nativeQuery = true)
    List<OutboxEvent> findDeadLetteredBetween(
        @Param("from") Instant from,
        @Param("to") Instant to,
        @Param("limit") int limit);

    @Query("SELECT o FROM OutboxEvent o WHERE o.id IN :ids AND o.deadLettered = TRUE")
    List<OutboxEvent> findDeadLetteredByIds(@Param("ids") List<UUID> ids);
}
