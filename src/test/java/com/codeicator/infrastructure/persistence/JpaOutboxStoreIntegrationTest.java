package com.codeicator.infrastructure.persistence;

import com.codeicator.infrastructure.OutboxEvent;
import com.codeicator.infrastructure.OutboxStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = JpaOutboxStoreIntegrationTest.TestApplication.class)
class JpaOutboxStoreIntegrationTest {

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EntityScan(basePackageClasses = OutboxEvent.class)
    @EnableJpaRepositories(basePackageClasses = OutboxEventRepository.class)
    static class TestApplication {
        @Bean
        OutboxStore outboxStore(OutboxEventRepository repository) {
            return new JpaOutboxStore(repository);
        }
    }

    @org.springframework.beans.factory.annotation.Autowired
    private OutboxStore outboxStore;

    @org.springframework.beans.factory.annotation.Autowired
    private OutboxEventRepository repository;

    @Test
    void getUnpublishedRespectsNextAttemptAt() {
        OutboxEvent ready = buildEvent(UUID.randomUUID(), Instant.now().minusSeconds(5), null);
        OutboxEvent delayed = buildEvent(UUID.randomUUID(), Instant.now().minusSeconds(5), Instant.now().plusSeconds(60));

        outboxStore.save(ready);
        outboxStore.save(delayed);

        List<OutboxEvent> unpublished = outboxStore.getUnpublished(10);

        assertTrue(unpublished.stream().anyMatch(event -> event.getId().equals(ready.getId())));
        assertFalse(unpublished.stream().anyMatch(event -> event.getId().equals(delayed.getId())));
    }

    @Test
    void recordFailureUpdatesNextAttemptAtAndRetryCount() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = buildEvent(eventId, Instant.now().minusSeconds(1), null);
        outboxStore.save(event);

        Instant nextAttempt = Instant.now().plusSeconds(30);
        outboxStore.recordFailure(eventId, "test failure", nextAttempt);

        OutboxEvent updated = repository.findById(eventId).orElse(null);
        assertNotNull(updated);
        assertEquals(1, updated.getRetryCount());
        assertEquals(
            nextAttempt.truncatedTo(ChronoUnit.MILLIS),
            updated.getNextAttemptAt().truncatedTo(ChronoUnit.MILLIS)
        );
    }

    private OutboxEvent buildEvent(UUID id, Instant createdAt, Instant nextAttemptAt) {
        return OutboxEvent.builder()
            .id(id)
            .eventType("com.codeicator.messages.Event")
            .eventPayload("{}").createdAt(createdAt)
            .retryCount(0)
            .deadLettered(false)
            .nextAttemptAt(nextAttemptAt)
            .build();
    }
}
