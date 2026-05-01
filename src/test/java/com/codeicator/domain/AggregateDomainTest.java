package com.codeicator.domain;

import com.codeicator.messages.Event;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateDomainTest {

    @SuperBuilder(toBuilder = true)
    private static class TestEvent extends Event {
    }

    private static class TestDomain extends Aggregate.Domain {
    }

    @Test
    void raiseDomainEventTracksVersionAndUncommittedEvents() {
        TestDomain domain = new TestDomain();
        TestEvent event = TestEvent.builder()
            .streamId("stream-1")
            .streamType("test")
            .correlationId("corr-1")
            .orderId(1)
            .build();

        domain.raiseDomainEvent(event);

        assertEquals(1, domain.getUncommittedEvents().size());
        assertEquals(1, domain.getVersion());
        assertEquals(0, event.getVersion());
        assertTrue(domain.getUncommittedEvents().contains(event));
    }
}

