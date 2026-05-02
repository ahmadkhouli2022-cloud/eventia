package com.codeicator.domain;

import com.codeicator.messages.DomainEvent;
import com.codeicator.messages.Event;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.UUID;

import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateDomainTest {

    @SuperBuilder(toBuilder = true)
    private static class TestEvent extends DomainEvent {
    }

    private static class TestDomain extends Aggregate.Domain {
    }

    @Test
    void raiseDomainEventTracksVersionAndUncommittedEvents() {
        TestDomain domain = new TestDomain();
        ReflectionTestUtils.setField(domain, "id", UUID.randomUUID());
        TestEvent event = TestEvent.builder()
            .correlationId("corr-1")
            .orderId(1)
            .build();

        domain.raiseDomainEvent(event);

        assertEquals(1, domain.getUncommittedEvents().size());
        assertEquals(1, domain.getVersion());
        Event persistedEvent = domain.getUncommittedEvents().getFirst();
        assertEquals(0, persistedEvent.getVersion());
        assertTrue(persistedEvent instanceof TestEvent);
    }
}
