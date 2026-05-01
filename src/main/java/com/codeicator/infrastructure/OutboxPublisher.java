package com.codeicator.infrastructure;

import com.codeicator.domain.EventPublisher;
import com.codeicator.domain.EventPublishingException;
import com.codeicator.infrastructure.reactivebus.Bus;
import com.codeicator.messages.Event;
import com.codeicator.messages.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

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
public class OutboxPublisher implements EventPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final long DEFAULT_RETRY_BACKOFF_MILLIS = 1000L;

    private final OutboxStore outboxStore;
    private final ObjectMapper objectMapper;
    private final Bus<Message> bus;
    private final int maxRetries;
    private final int batchSize;
    private final long retryBackoffMillis;
    private final OutboxMetricsRecorder metricsRecorder;

    public OutboxPublisher(
        OutboxStore outboxStore,
        ObjectMapper objectMapper,
        Bus<Message> bus) {
        this(outboxStore, objectMapper, bus, DEFAULT_MAX_RETRIES, DEFAULT_BATCH_SIZE, DEFAULT_RETRY_BACKOFF_MILLIS, new NoopOutboxMetricsRecorder());
    }

    public OutboxPublisher(
        OutboxStore outboxStore,
        ObjectMapper objectMapper,
        Bus<Message> bus,
        int maxRetries,
        int batchSize) {
        this(outboxStore, objectMapper, bus, maxRetries, batchSize, DEFAULT_RETRY_BACKOFF_MILLIS, new NoopOutboxMetricsRecorder());
    }

    public OutboxPublisher(
        OutboxStore outboxStore,
        ObjectMapper objectMapper,
        Bus<Message> bus,
        int maxRetries,
        int batchSize,
        long retryBackoffMillis) {
        this(outboxStore, objectMapper, bus, maxRetries, batchSize, retryBackoffMillis, new NoopOutboxMetricsRecorder());
    }

    public OutboxPublisher(
        OutboxStore outboxStore,
        ObjectMapper objectMapper,
        Bus<Message> bus,
        int maxRetries,
        int batchSize,
        long retryBackoffMillis,
        OutboxMetricsRecorder metricsRecorder) {
        this.outboxStore = outboxStore;
        this.objectMapper = objectMapper;
        this.bus = bus;
        this.maxRetries = maxRetries;
        this.batchSize = batchSize;
        this.retryBackoffMillis = retryBackoffMillis;
        this.metricsRecorder = metricsRecorder == null ? new NoopOutboxMetricsRecorder() : metricsRecorder;
    }

    public void publishPending() {
        try {
            List<OutboxEvent> unpublished = outboxStore.getUnpublished(batchSize);

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
            bus.raiseEvent(event);

            // Mark as published in outbox store
            outboxStore.markAsPublished(outboxEvent.getId());
            metricsRecorder.recordPublishSuccess();

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

        if (outboxEvent.getRetryCount() >= maxRetries) {
            log.error("Max retries ({}) exceeded for outbox event {}. " +
                    "Moving to dead letter queue. Reason: {}",
                maxRetries,
                outboxEvent.getId(),
                reason);

            outboxStore.moveToDeadLetter(outboxEvent);
            metricsRecorder.recordDeadLetter();
            logDeadLetterEvent(outboxEvent, reason);
        } else {
            Instant nextAttemptAt = Instant.now().plusMillis(retryBackoffMillis);
            outboxStore.recordFailure(outboxEvent.getId(), reason, nextAttemptAt);
            metricsRecorder.recordPublishFailure();
            log.info("Event {} queued for retry at {}. Attempt {} of {}",
                outboxEvent.getId(),
                nextAttemptAt,
                outboxEvent.getRetryCount() + 1,
                maxRetries);
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

    public void replayDeadLettered(int limit) {
        try {
            List<OutboxEvent> deadLettered = outboxStore.getDeadLetterEvents(limit);
            if (deadLettered.isEmpty()) {
                return;
            }

            for (OutboxEvent event : deadLettered) {
                publishEvent(event);
            }
        } catch (Exception e) {
            log.error("Failed to replay dead-lettered events", e);
        }
    }

    public void replayDeadLetteredBetween(Instant from, Instant to, int limit) {
        try {
            List<OutboxEvent> deadLettered = outboxStore.getDeadLetterEventsBetween(from, to, limit);
            if (deadLettered.isEmpty()) {
                return;
            }

            for (OutboxEvent event : deadLettered) {
                publishEvent(event);
            }
        } catch (Exception e) {
            log.error("Failed to replay dead-lettered events by time range", e);
        }
    }

    public void replayDeadLetteredByIds(List<UUID> ids) {
        try {
            List<OutboxEvent> deadLettered = outboxStore.getDeadLetterEventsByIds(ids);
            if (deadLettered.isEmpty()) {
                return;
            }

            for (OutboxEvent event : deadLettered) {
                publishEvent(event);
            }
        } catch (Exception e) {
            log.error("Failed to replay dead-lettered events by ids", e);
        }
    }

    @Override
    public void publish(Event event) throws EventPublishingException {
        var envelop = OutboxEvent.from(event,objectMapper);
        outboxStore.save(envelop);
    }
}
