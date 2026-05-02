package com.codeicator.domain;

import com.codeicator.infrastructure.reactivebus.DomainEventHandler;
import com.codeicator.infrastructure.reactivebus.annotations.HandleDomainEvent;
import com.codeicator.messages.DomainEvent;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AggregateUnitTest {

    @SuperBuilder(toBuilder = true)
    private static class TestEvent extends DomainEvent {
    }

    private static class TestDomain extends Aggregate.Domain {
    }

    private static class TestAggregate extends Aggregate<TestDomain> {
        TestAggregate(DataPersistent<TestDomain> persistent, ApplicationContext context) {
            super(persistent, context);
        }
    }

    private static class TestHandler {
        private final AtomicInteger count = new AtomicInteger(0);

        @HandleDomainEvent(messageType = TestEvent.class)
        public void handle(TestEvent event) {
            count.incrementAndGet();
        }
    }

    @BeforeEach
    void resetHandlers() {
        ReflectionTestUtils.setField(Aggregate.class, "handlersInitialized", false);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, List<DomainEventHandler>> handlersMap =
            (ConcurrentHashMap<String, List<DomainEventHandler>>) ReflectionTestUtils.getField(Aggregate.class, "handlersMap");
        if (handlersMap != null) {
            handlersMap.clear();
        }
    }

    @Test
    void aggregateInvokesAnnotatedHandlers() {
        TestHandler handler = new TestHandler();
        ApplicationContext context = mock(ApplicationContext.class);
        when(context.getBeansOfType(Object.class)).thenReturn(Map.of("testHandler", handler));

        DataPersistent<TestDomain> persistent = domain -> domain;
        TestAggregate aggregate = new TestAggregate(persistent, context);

        TestDomain domain = new TestDomain();
        ReflectionTestUtils.setField(domain, "id", UUID.randomUUID());
        TestEvent event = TestEvent.builder()
            .correlationId("corr-1")
            .orderId(1)
            .build();

        domain.raiseDomainEvent(event);
        aggregate.aggregate(domain);

        assertEquals(1, handler.count.get());
        assertEquals(0, domain.getUncommittedEvents().size());
    }
}
