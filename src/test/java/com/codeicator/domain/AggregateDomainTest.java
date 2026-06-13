package com.codeicator.domain;

import com.codeicator.messages.Event;
import jakarta.persistence.Id;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.UUID;

import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateDomainTest {

    @SuperBuilder(toBuilder = true)
    private static class TestEvent extends Event {
    }

    private static class TestDomain extends Aggregate.Domain {
            @Id
            private UUID id;
    }

    @Test
    void raiseDomainEventTracksVersionAndUncommittedEvents() {
        TestDomain domain = new TestDomain();
        UUID id = UUID.randomUUID();
      domain.id=id;
        TestEvent event = TestEvent.builder()
            .streamId(String.valueOf(id))
            .streamType(domain.getClass().getName())
            .correlationId("corr-1")
            .orderId(1)
            .build();

        domain.raiseDomainEvent(event);

        assertEquals(1, domain.getUncommittedEvents().size());
        assertEquals(1, domain.getVersion());
        Event persistedEvent = domain.getUncommittedEvents().getFirst();
        assertEquals(1, persistedEvent.getVersion());
        assertTrue(persistedEvent instanceof TestEvent);
    }
}
