package com.codeicator.domain;

import com.codeicator.messages.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final int MAX_RETRIES = 3;
    private static final int BATCH_SIZE = 100;

    private final OutboxStore outboxStore;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper; // ✅ Add ObjectMapper

    public OutboxPublisher(
        OutboxStore outboxStore,
        EventPublisher eventPublisher,
        ObjectMapper objectMapper) {  // ✅ Inject ObjectMapper
        this.outboxStore = outboxStore;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
    }

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
        }
    }

    private void publishEvent(OutboxEvent outboxEvent) {
        try {
            log.debug("Publishing outbox event: {} (type: {})",
                outboxEvent.getId(),
                outboxEvent.getEventType());

            // ✅ Deserialize event from JSON
            Event event = outboxEvent.getEvent(objectMapper);

            // Publish to event bus
            eventPublisher.publish(event);

            // Mark as published in outbox store
            outboxStore.markAsPublished(outboxEvent.getId());

            log.info("Successfully published outbox event: {} after {} attempt(s)",
                outboxEvent.getId(),
                outboxEvent.getRetryCount() + 1);

        } catch (Exception e) {
            handlePublishFailure(outboxEvent, e);
        }
    }

    private void handlePublishFailure(OutboxEvent outboxEvent, Exception exception) {
        String reason = exception.getMessage() != null
            ? exception.getMessage()
            : exception.getClass().getSimpleName();

        log.warn("Failed to publish outbox event {}: {}",
            outboxEvent.getId(),
            reason);

        if (outboxEvent.getRetryCount() >= MAX_RETRIES) {
            log.error("Max retries ({}) exceeded for outbox event {}. " +
                    "Moving to dead letter queue. Reason: {}",
                MAX_RETRIES,
                outboxEvent.getId(),
                reason);

            outboxStore.moveToDeadLetter(outboxEvent);
            logDeadLetterEvent(outboxEvent, reason);
        } else {
            outboxStore.recordFailure(outboxEvent.getId(), reason);
            log.info("Event {} queued for retry. Attempt {} of {}",
                outboxEvent.getId(),
                outboxEvent.getRetryCount() + 1,
                MAX_RETRIES);
        }
    }

    private void logDeadLetterEvent(OutboxEvent event, String reason) {
        try {
            log.error("DEAD_LETTER: event_id={}, event_type={}, reason={}, age_ms={}",
                event.getId(),
                event.getEventType(),
                reason,
                event.getAgeMillis());
        } catch (Exception e) {
            log.error("Failed to log dead letter event", e);
        }
    }

    public int getUnpublishedCount() {
        try {
            return outboxStore.getUnpublished(Integer.MAX_VALUE).size();
        } catch (Exception e) {
            log.error("Failed to get unpublished count", e);
            return -1;
        }
    }

    public int getDeadLetterCount() {
        try {
            return outboxStore.getDeadLetterEvents().size();
        } catch (Exception e) {
            log.error("Failed to get dead letter count", e);
            return -1;
        }
    }
}
