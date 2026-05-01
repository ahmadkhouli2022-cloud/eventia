package com.codeicator.infrastructure.config;

import com.codeicator.infrastructure.MicrometerOutboxMetricsRecorder;
import com.codeicator.infrastructure.NoopOutboxMetricsRecorder;
import com.codeicator.infrastructure.OutboxMetricsRecorder;
import com.codeicator.infrastructure.OutboxPublisher;
import com.codeicator.infrastructure.OutboxStore;
import com.codeicator.infrastructure.reactivebus.Bus;
import com.codeicator.messages.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
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

    @Value("${outbox.max-retries:3}")
    private int maxRetries;

    @Value("${outbox.batch-size:100}")
    private int batchSize;

    @Value("${outbox.cleanup-retention-days:30}")
    private long retentionDays;

    @Value("${outbox.retry-backoff-ms:1000}")
    private long retryBackoffMillis;

    /**
     * Create OutboxMetricsRecorder bean.
     *
     * @param meterRegistry Micrometer registry used for metrics
     * @return configured OutboxMetricsRecorder
     */
    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    public OutboxMetricsRecorder outboxMetricsRecorder(MeterRegistry meterRegistry) {
        return new MicrometerOutboxMetricsRecorder(meterRegistry);
    }

    @Bean
    public OutboxMetricsRecorder fallbackOutboxMetricsRecorder() {
        return new NoopOutboxMetricsRecorder();
    }

    @Bean
    public OutboxPublisher outboxPublisher(
        OutboxStore outboxStore,
        Bus<Message> bus,
        ObjectMapper objectMapper,
        OutboxMetricsRecorder outboxMetricsRecorder) {

        log.info("Creating OutboxPublisher bean");
        return new OutboxPublisher(
            outboxStore,
            objectMapper,
            bus,
            maxRetries,
            batchSize,
            retryBackoffMillis,
            outboxMetricsRecorder
        );
    }

    @Bean
    public OutboxPollingJob outboxPollingJob(
        OutboxPublisher outboxPublisher,
        OutboxStore outboxStore) {
        log.info("Creating OutboxPollingJob bean");
        return new OutboxPollingJob(outboxPublisher, outboxStore, retentionDays);
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
        private final OutboxStore outboxStore;
        private final long retentionDays;

        public OutboxPollingJob(
            OutboxPublisher outboxPublisher,
            OutboxStore outboxStore,
            long retentionDays) {
            this.outboxPublisher = outboxPublisher;
            this.outboxStore = outboxStore;
            this.retentionDays = retentionDays;
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
                long thresholdMillis = System.currentTimeMillis() -
                    (retentionDays * 24 * 60 * 60 * 1000);

                long deleted = outboxStore.deletePublishedBefore(thresholdMillis);
                log.info("Deleted {} published outbox events older than {} days", deleted, retentionDays);
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
