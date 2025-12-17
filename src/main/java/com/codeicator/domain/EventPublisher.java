package com.codeicator.domain;

import com.codeicator.messages.Event;

/**
 * Publishes domain events to event subscribers.
 *
 * IMPLEMENTATION REQUIREMENTS:
 *
 * 1. Async Publishing:
 *    - Should publish asynchronously to avoid blocking
 *    - Consider using Spring Cloud Stream or similar
 *
 * 2. Error Handling:
 *    - Throw EventPublishingException on failure
 *    - Include root cause in exception
 *    - Do not silently ignore failures
 *
 * 3. Reliability:
 *    - Guarantee at-least-once delivery
 *    - Use message broker with persistence
 *    - Consider implementing with Outbox Pattern
 *
 * 4. Thread Safety:
 *    - Must be thread-safe for concurrent publishing
 *    - Can block caller (aggregate handles async if needed)
 *
 * 5. Event Ordering:
 *    - Maintain order per stream/aggregate
 *    - Use streamId from Event for partitioning
 *
 * EXAMPLE IMPLEMENTATIONS:
 * - Spring Cloud Stream adapter
 * - Kafka producer wrapper
 * - RabbitMQ publisher
 * - In-memory event bus (for testing)
 */
public interface EventPublisher {

    /**
     * Publish a domain event.
     *
     * Must be thread-safe and handle concurrent calls.
     *
     * @param event The event to publish
     * @throws EventPublishingException if publishing fails
     * @throws IllegalArgumentException if event is null
     */
    void publish(Event event) throws EventPublishingException;
}
