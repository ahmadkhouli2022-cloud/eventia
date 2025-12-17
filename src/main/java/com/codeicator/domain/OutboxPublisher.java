package com.codeicator.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Polls and publishes outbox events.
 *
 * Should be run as a scheduled job (@Scheduled) every 1-5 seconds.
 * Works in conjunction with the Aggregate and DataPersistent to implement
 * the outbox pattern for guaranteed event delivery.
 *
 * Guarantees exactly-once delivery semantics through:
 * 1. Persistent outbox storage (events survive crashes)
 * 2. Atomic mark-as-published (prevents duplicate publishing)
 * 3. Automatic retry with configurable max attempts
 * 4. Dead letter queue for permanently failed events
 *
 * Architecture:
 * ┌─────────────┐
 * │  Aggregate  │ (receives commands)
 * └──────┬──────┘
 *        │ domain.getUncommittedEvents()
 *        ▼
 * ┌──────────────────┐
 * │ DataPersistent   │ (persists state + events in one transaction)
 * └──────┬───────────┘
 *        │ outboxStore.saveAll(events)
 *        ▼
 * ┌──────────────────┐
 * │   Outbox Table   │ (durable event storage)
 * └──────┬───────────┘
 *        │ (polled by OutboxPublisher)
 *        ▼
 * ┌──────────────────┐
 * │ OutboxPublisher  │ ← THIS CLASS
 * └──────┬───────────┘
 *        │
 *     ┌──┴──┐
 *     ▼     ▼
 * [Success] [Failure]
 *     │        │
 *     │        ├─→ recordFailure()
 *     │        │
 *     │        └─→ if(retries > max): moveToDeadLetter()
 *     │
 *     └─→ markAsPublished()
 *           │
 *           ▼
 *     [Event Bus/Kafka/RabbitMQ]
 *
 * @see OutboxEvent
 * @see OutboxStore
 * @see EventPublisher
 */
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    /**
     * Maximum number of retry attempts before moving to dead letter queue
     */
    private static final int MAX_RETRIES = 3;

    /**
     * Maximum number of events to process in one polling cycle
     */
    private static final int BATCH_SIZE = 100;

    /**
     * Storage for outbox events
     */
    private final OutboxStore outboxStore;

    /**
     * Publisher for sending events to event bus
     */
    private final EventPublisher eventPublisher;

    /**
     * Create new OutboxPublisher
     * @param outboxStore the outbox storage implementation
     * @param eventPublisher the event publisher (Spring Cloud Stream, Kafka, etc)
     */
    public OutboxPublisher(OutboxStore outboxStore, EventPublisher eventPublisher) {
        this.outboxStore = outboxStore;
        this.eventPublisher = eventPublisher;
    }

    /**
     * Poll and publish pending outbox events.
     *
     * Should be called regularly from a scheduled job:
     * <pre>
     * @Scheduled(fixedRate = 1000) // Every 1 second
     * public void pollAndPublish() {
     *     outboxPublisher.publishPending();
     * }
     * </pre>
     *
     * Polling rate recommendations:
     * - High-throughput: 500-1000ms
     * - Normal: 1-5 seconds
     * - Low-throughput: 5-30 seconds
     *
     * Each call:
     * 1. Fetches up to BATCH_SIZE unpublished events
     * 2. Attempts to publish each event
     * 3. Marks successful publishes
     * 4. Retries or dead-letters failed events
     */
    public void publishPending() {
        try {
            List<OutboxEvent> unpublished = outboxStore.getUnpublished(BATCH_SIZE);

            if (unpublished.isEmpty()) {
                log.trace("No unpublished outbox events found");
                return;
            }

            log.debug("Found {} unpublished events in outbox", unpublished.size());

            for (OutboxEvent outboxEvent : unpublished) {
                publishEvent(outboxEvent);
            }
        } catch (Exception e) {
            log.error("Unexpected error during outbox publishing", e);
            // Don't throw - allow next polling cycle to retry
        }
    }

    /**
     * Attempt to publish a single outbox event.
     *
     * Flow:
     * 1. Try to publish event to event bus
     * 2. If successful: mark as published
     * 3. If failed:
     *    a. If retries remaining: record failure (will retry next cycle)
     *    b. If retries exhausted: move to dead letter queue
     *
     * @param outboxEvent the event to publish
     */
    private void publishEvent(OutboxEvent outboxEvent) {
        try {
            log.debug("Publishing outbox event: {} (type: {})",
                outboxEvent.getId(),
                outboxEvent.getEvent().getClass().getSimpleName());

            // Publish to event bus (Spring Cloud Stream, Kafka, RabbitMQ, etc)
            eventPublisher.publish(outboxEvent.getEvent());

            // Mark as published in outbox store
            outboxStore.markAsPublished(outboxEvent.getId());

            log.info("Successfully published outbox event: {} after {} attempt(s)",
                outboxEvent.getId(),
                outboxEvent.getRetryCount() + 1);

        } catch (Exception e) {
            handlePublishFailure(outboxEvent, e);
        }
    }

    /**
     * Handle failure of event publishing.
     *
     * @param outboxEvent the event that failed to publish
     * @param exception the exception that occurred
     */
    private void handlePublishFailure(OutboxEvent outboxEvent, Exception exception) {
        String reason = exception.getMessage() != null
            ? exception.getMessage()
            : exception.getClass().getSimpleName();

        log.warn("Failed to publish outbox event {}: {}",
            outboxEvent.getId(),
            reason);

        // Check if we should retry
        if (outboxEvent.getRetryCount() >= MAX_RETRIES) {
            log.error("Max retries ({}) exceeded for outbox event {}. " +
                "Moving to dead letter queue. Reason: {}",
                MAX_RETRIES,
                outboxEvent.getId(),
                reason);

            outboxStore.moveToDeadLetter(outboxEvent);

            // Log for monitoring/alerting
            logDeadLetterEvent(outboxEvent, reason);
        } else {
            // Record failure and will retry in next cycle
            outboxStore.recordFailure(outboxEvent.getId(), reason);

            log.info("Event {} queued for retry. Attempt {} of {}",
                outboxEvent.getId(),
                outboxEvent.getRetryCount() + 1,
                MAX_RETRIES);
        }
    }

    /**
     * Log dead letter event for monitoring and alerting.
     * In production, this should send to monitoring system.
     *
     * @param event the dead lettered event
     * @param reason why it failed
     */
    private void logDeadLetterEvent(OutboxEvent event, String reason) {
        try {
            log.error("DEAD_LETTER: event_id={}, event_type={}, reason={}, age_ms={}",
                event.getId(),
                event.getEvent().getClass().getSimpleName(),
                reason,
                event.getAgeMillis());

            // TODO: Send to monitoring system (DataDog, Prometheus, etc)
            // TODO: Create alert if too many events in DLQ
            // TODO: Set up dashboard to monitor DLQ
        } catch (Exception e) {
            log.error("Failed to log dead letter event", e);
        }
    }

    /**
     * Get count of unpublished events (for monitoring).
     *
     * @return number of events waiting to be published
     */
    public int getUnpublishedCount() {
        try {
            return outboxStore.getUnpublished(Integer.MAX_VALUE).size();
        } catch (Exception e) {
            log.error("Failed to get unpublished count", e);
            return -1;
        }
    }

    /**
     * Get count of dead lettered events (for monitoring).
     *
     * @return number of events in dead letter queue
     */
    public int getDeadLetterCount() {
        try {
            return outboxStore.getDeadLetterEvents().size();
        } catch (Exception e) {
            log.error("Failed to get dead letter count", e);
            return -1;
        }
    }
}

