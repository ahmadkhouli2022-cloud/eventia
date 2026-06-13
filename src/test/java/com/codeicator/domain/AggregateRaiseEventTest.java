package com.codeicator.domain;

import com.codeicator.messages.Event;
import lombok.extern.jackson.Jacksonized;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class AggregateRaiseEventTest {

    @Jacksonized
    @SuperBuilder(toBuilder = true)
    private static class TestEventWithToBuilder extends Event {
    }

    @Jacksonized
    @SuperBuilder // no toBuilder() generated
    private static class TestEventNoToBuilder extends Event {
    }

    private static class TestDomain extends Aggregate.Domain {
        // empty - use reflection to set id/version if needed
    }

    private void setDomainId(Aggregate.Domain domain, UUID id) throws Exception {
        Field f = Aggregate.Domain.class.getDeclaredField("id");
        f.setAccessible(true);
        f.set(domain, id);
    }

    @Test
    void raiseEvent_usingToBuilder_shouldAttachRebuiltEvent() throws Exception {
        TestDomain domain = new TestDomain();
        setDomainId(domain, UUID.randomUUID());

        TestEventWithToBuilder e = TestEventWithToBuilder.builder()
            .streamId("original-stream")
            .streamType("original-type")
            .correlationId("corr")
            .orderId(1)
            .build();

        domain.raiseDomainEvent(e);

        List<Event> events = domain.getUncommittedEvents();
        assertEquals(1, events.size());
        Event attached = events.get(0);
        assertNotNull(attached.getId());
        // streamType should be overwritten with the domain class name
        assertEquals(domain.getClass().getName(), attached.getStreamType());
        // when built, event.version equals pre-increment domain.version (0), then domain.version becomes 1
        assertEquals(0L, attached.getVersion());
        assertEquals(1L, domain.getVersion());
        assertEquals(String.valueOf(getDomainId(domain)), attached.getStreamId());
    }

    @Test
    void raiseEvent_noToBuilder_fallbackToStaticBuilder_shouldAttachRebuiltEvent() throws Exception {
        TestDomain domain = new TestDomain();
        setDomainId(domain, UUID.randomUUID());

        TestEventNoToBuilder e = TestEventNoToBuilder.builder()
            .streamId("original-stream")
            .streamType("original-type")
            .correlationId("corr")
            .orderId(2)
            .build();

        // Ensure the event class does not have toBuilder()
        boolean hasToBuilder = false;
        try {
            e.getClass().getMethod("toBuilder");
            hasToBuilder = true;
        } catch (NoSuchMethodException ignored) { }
        assertFalse(hasToBuilder, "TestEventNoToBuilder unexpectedly has toBuilder()");

        domain.raiseDomainEvent(e);

        List<Event> events = domain.getUncommittedEvents();
        assertEquals(1, events.size());
        Event attached = events.get(0);
        assertNotNull(attached.getId());
        assertEquals(domain.getClass().getName(), attached.getStreamType());
        assertEquals(0L, attached.getVersion());
        assertEquals(1L, domain.getVersion());
        assertEquals(String.valueOf(getDomainId(domain)), attached.getStreamId());
    }

    private UUID getDomainId(Aggregate.Domain domain) throws Exception {
        Field f = Aggregate.Domain.class.getDeclaredField("id");
        f.setAccessible(true);
        return (UUID) f.get(domain);
    }
}

