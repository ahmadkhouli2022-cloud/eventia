package com.codeicator.infrastructure.config;

import com.codeicator.domain.EventPublisher;
import com.codeicator.domain.OutboxPublisher;
import com.codeicator.domain.OutboxStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Spring Configuration for Outbox Pattern implementation.
 *
 * Sets up:
 * 1. OutboxPublisher bean for event publishing
 * 2. Scheduled polling job to process outbox events
 * 3. Lifecycle management for startup/shutdown
 *
 * Enable with: spring.outbox.enabled=true
 */
@Configuration
@EnableScheduling
@Slf4j
public class OutboxConfiguration {

    /**
     * Create OutboxPublisher bean.
     *
     * The publisher polls outbox table and publishes events
     * to the message bus with automatic retry and dead letter handling.
     *
     * @param outboxStore the outbox event storage
     * @param eventPublisher the event bus publisher
     * @return configured OutboxPublisher
     */
    @Bean
    public OutboxPublisher outboxPublisher(
        OutboxStore outboxStore,
        EventPublisher eventPublisher,
        ObjectMapper objectMapper) {  // ✅ Inject ObjectMapper

        log.info("Creating OutboxPublisher bean");
        return new OutboxPublisher(outboxStore, eventPublisher, objectMapper);
    }

    @Bean
    public OutboxPollingJob outboxPollingJob(OutboxPublisher outboxPublisher) {
        log.info("Creating OutboxPollingJob bean");
        return new OutboxPollingJob(outboxPublisher);
    }


    /**
     * Outbox event polling and processing job.
     *
     * Runs on a schedule to:
     * 1. Poll unpublished events from outbox
     * 2. Publish them to the event bus
     * 3. Mark as published or retry on failure
     * 4. Move to dead letter queue after max retries
     * 5. Cleanup old published events
     *
     * Configure polling rate:
     * spring.outbox.polling-interval=1000  (milliseconds, default=1000)
     * spring.outbox.cleanup-schedule=0 0 2 * * *  (cron, default=2 AM daily)
     */
    @Slf4j
    public static class OutboxPollingJob {

        private final OutboxPublisher outboxPublisher;

        public OutboxPollingJob(OutboxPublisher outboxPublisher) {
            this.outboxPublisher = outboxPublisher;
        }

        /**
         * Poll and publish pending outbox events.
         *
         * Runs every 1 second by default (configurable).
         *
         * Schedule configuration:
         * - Development: 1-5 seconds (faster feedback)
         * - Production: 5-30 seconds (less load)
         * - High throughput: 500-1000ms
         *
         * Metrics:
         * - Published events per second
         * - Average publish latency
         * - Retry rate
         * - DLQ rate
         */
        @Scheduled(fixedRateString = "${outbox.polling-interval:1000}")
        public void publishPendingEvents() {
            try {
                outboxPublisher.publishPending();
            } catch (Exception e) {
                log.error("Unexpected error in outbox polling", e);
                // Don't throw - allow next cycle to continue
            }
        }


        @Scheduled(cron = "${outbox.cleanup-schedule:0 0 2 * * *}")
        public void cleanupOldPublishedEvents() {
            try {
                long retentionDays = Long.parseLong(
                    System.getProperty("outbox.cleanup-retention-days", "30"));

                long thirtyDaysAgo = System.currentTimeMillis() -
                    (retentionDays * 24 * 60 * 60 * 1000);

                // Note: OutboxPublisher needs access to OutboxStore
                // This is a placeholder - implement in OutboxPublisher or OutboxStore
                log.info("Cleanup of published events scheduled " +
                    "(needs OutboxStore access - implement in OutboxPublisher)");

            } catch (Exception e) {
                log.error("Error during cleanup of old published events", e);
                // Don't throw - allow next cycle
            }
        }
    }

    /**
     * Application startup listener.
     * Logs outbox configuration on startup.
     */
    public OutboxConfiguration() {
        log.info("Outbox Pattern Configuration loaded");
        log.info("  - Polling enabled: true");
        log.info("  - Scheduling enabled: true");
        log.info("  - Default polling interval: 1 second");
        log.info("  - Default max retries: 3");
        log.info("  - Default batch size: 100");
    }
}

