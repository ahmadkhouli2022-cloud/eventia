package com.codeicator.infrastructure.config;

import com.codeicator.infrastructure.OutboxPublisher;
import com.codeicator.infrastructure.OutboxStore;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class OutboxPollingJobTest {

    @Test
    void cleanupIsSkippedWhenDisabled() {
        OutboxPublisher publisher = mock(OutboxPublisher.class);
        OutboxStore store = mock(OutboxStore.class);

        // Create job with cleanup disabled
        OutboxConfiguration.OutboxPollingJob job =
            new OutboxConfiguration.OutboxPollingJob(publisher, store, 30L, false);

        // Call cleanup method
        job.cleanupOldPublishedEvents();

        // Verify that deletePublishedBefore was never called
        verify(store, never()).deletePublishedBefore(anyLong());
    }
}

