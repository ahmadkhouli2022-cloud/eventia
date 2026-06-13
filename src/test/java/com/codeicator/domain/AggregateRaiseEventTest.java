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
        UUID id = UUID.randomUUID();
        setDomainId(domain, id);

        // Build the event with correct metadata (now the aggregate validates instead of overriding)
        TestEventWithToBuilder e = TestEventWithToBuilder.builder()
            .streamId(String.valueOf(id))
            .streamType(domain.getClass().getName())
            .correlationId("corr")
            .orderId(1)
            .build();

        domain.raiseDomainEvent(e);

        List<Event> events = domain.getUncommittedEvents();
        assertEquals(1, events.size());
        Event attached = events.get(0);
        assertNotNull(attached.getId());
        // streamType should match the provided domain class name
        assertEquals(domain.getClass().getName(), attached.getStreamType());
        // event.version defaults to 1 (no overwrite), domain version increments to 1
        assertEquals(1L, attached.getVersion());
        assertEquals(1L, domain.getVersion());
        assertEquals(String.valueOf(id), attached.getStreamId());
    }

    @Test
    void raiseEvent_noToBuilder_fallbackToStaticBuilder_shouldAttachRebuiltEvent() throws Exception {
        TestDomain domain = new TestDomain();
        UUID id = UUID.randomUUID();
        setDomainId(domain, id);

        TestEventNoToBuilder e = TestEventNoToBuilder.builder()
            .streamId(String.valueOf(id))
            .streamType(domain.getClass().getName())
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
        assertEquals(1L, attached.getVersion());
        assertEquals(1L, domain.getVersion());
        assertEquals(String.valueOf(id), attached.getStreamId());
    }

    @Test
    void raiseEvent_invalidStreamType_shouldBeRejected() throws Exception {
        TestDomain domain = new TestDomain();
        UUID id = UUID.randomUUID();
        setDomainId(domain, id);

        TestEventWithToBuilder e = TestEventWithToBuilder.builder()
            .streamId(String.valueOf(id))
            .streamType("some.other.Type")
            .build();

        assertThrows(IllegalArgumentException.class, () -> domain.raiseDomainEvent(e));
    }

    @Test
    void raiseEvent_invalidStreamId_shouldBeRejected() throws Exception {
        TestDomain domain = new TestDomain();
        UUID id = UUID.randomUUID();
        setDomainId(domain, id);

        TestEventWithToBuilder e = TestEventWithToBuilder.builder()
            .streamId("wrong-id")
            .streamType(domain.getClass().getName())
            .build();

        assertThrows(IllegalArgumentException.class, () -> domain.raiseDomainEvent(e));
    }

    private UUID getDomainId(Aggregate.Domain domain) throws Exception {
        Field f = Aggregate.Domain.class.getDeclaredField("id");
        f.setAccessible(true);
        return (UUID) f.get(domain);
    }
}

