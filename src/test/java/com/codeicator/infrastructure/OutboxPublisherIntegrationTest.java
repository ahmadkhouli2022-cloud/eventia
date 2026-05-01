package com.codeicator.infrastructure;

import com.codeicator.infrastructure.persistence.JpaOutboxStore;
import com.codeicator.infrastructure.persistence.OutboxEventRepository;
import com.codeicator.infrastructure.reactivebus.Bus;
import com.codeicator.messages.Event;
import com.codeicator.messages.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@SpringBootTest(classes = OutboxPublisherIntegrationTest.TestApplication.class)
class OutboxPublisherIntegrationTest {

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EntityScan(basePackageClasses = OutboxEvent.class)
    @EnableJpaRepositories(basePackageClasses = OutboxEventRepository.class)
    static class TestApplication {
        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        OutboxStore outboxStore(OutboxEventRepository repository) {
            return new JpaOutboxStore(repository);
        }

        @Bean
        OutboxPublisher outboxPublisher(
            OutboxStore outboxStore,
            ObjectMapper objectMapper,
            Bus<Message> bus) {
            return new OutboxPublisher(outboxStore, objectMapper, bus, 3, 50, 200);
        }
    }

    @Jacksonized
    @SuperBuilder(toBuilder = true)
    private static class TestEvent extends Event {
    }

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private OutboxEventRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private Bus<Message> bus;

    @Test
    void publishPendingMarksEventPublished() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = buildEvent(eventId, Instant.now().minusSeconds(2));
        repository.save(event);

        outboxPublisher.publishPending();

        OutboxEvent updated = repository.findById(eventId).orElse(null);
        assertNotNull(updated);
        assertNotNull(updated.getPublishedAt());
        assertEquals(0, updated.getRetryCount());
        verify(bus).raiseEvent(org.mockito.ArgumentMatchers.any(Event.class));
    }

    @Test
    void publishPendingSchedulesRetryOnFailure() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = buildEvent(eventId, Instant.now().minusSeconds(2));
        repository.save(event);

        doThrow(new RuntimeException("broker down"))
            .when(bus)
            .raiseEvent(org.mockito.ArgumentMatchers.any(Event.class));

        Instant before = Instant.now();
        outboxPublisher.publishPending();

        OutboxEvent updated = repository.findById(eventId).orElse(null);
        assertNotNull(updated);
        assertEquals(1, updated.getRetryCount());
        assertNotNull(updated.getNextAttemptAt());
        assertTrue(updated.getNextAttemptAt().isAfter(before));
    }

    private OutboxEvent buildEvent(UUID id, Instant createdAt) {
        TestEvent event = TestEvent.builder()
            .streamId(id.toString())
            .streamType("test")
            .correlationId("corr-" + id)
            .orderId(1)
            .build();

        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return OutboxEvent.builder()
            .id(id)
            .eventType(TestEvent.class.getName())
            .eventPayload(payload)
            .createdAt(createdAt)
            .retryCount(0)
            .deadLettered(false)
            .build();
    }
}
