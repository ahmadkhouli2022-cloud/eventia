package com.codeicator.domain;

import com.codeicator.infrastructure.reactivebus.DomainEventHandler;
import com.codeicator.infrastructure.reactivebus.annotations.HandleDomainEvent;
import com.codeicator.messages.Event;
import jakarta.persistence.Id;
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

class AggregateHandlersTest {

    @SuperBuilder(toBuilder = true)
    private static class TestEvent extends Event { }

    private static class TestDomain extends Aggregate.Domain {
        @Id
        private UUID id;
    }

    private static class TestAggregate extends Aggregate<TestDomain, UUID> {
        TestAggregate(DataPersistent<TestDomain> persistent, ApplicationContext context) {
            super(persistent, context);
        }
    }

    private static class HandlerA {
        final AtomicInteger count = new AtomicInteger();
        @HandleDomainEvent(messageType = TestEvent.class)
        public java.util.function.Consumer<TestEvent> topUpTransactionFailedEventHandler1() { return e -> count.incrementAndGet(); }
    }

    private static class HandlerB {
        final AtomicInteger count = new AtomicInteger();
        @HandleDomainEvent(messageType = TestEvent.class)
        public java.util.function.Consumer<TestEvent> topUpTransactionFailedEventHandler1() { return e -> count.incrementAndGet(); }
    }

    private static class FailingHandler {
        final AtomicInteger count = new AtomicInteger();
        @HandleDomainEvent(messageType = TestEvent.class)
        public java.util.function.Consumer<TestEvent> topUpTransactionFailedEventHandler1() { return e -> { count.incrementAndGet(); throw new RuntimeException("boom"); }; }
    }

    @BeforeEach
    void resetHandlers() {
        ReflectionTestUtils.setField(Aggregate.class, "handlersInitialized", false);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, List<DomainEventHandler>> handlersMap =
            (ConcurrentHashMap<String, List<DomainEventHandler>>) ReflectionTestUtils.getField(Aggregate.class, "handlersMap");
        if (handlersMap != null) handlersMap.clear();
    }

    @Test
    void multipleHandlersAreInvoked() {
        HandlerA a = new HandlerA();
        HandlerB b = new HandlerB();
        ApplicationContext context = mock(ApplicationContext.class);
        when(context.getBeansOfType(Object.class)).thenReturn(Map.of("a", a, "b", b));

        DataPersistent<TestDomain> persistent = domain -> domain;
        TestAggregate aggregate = new TestAggregate(persistent, context);

        TestDomain domain = new TestDomain();
        UUID id = UUID.randomUUID();
        ReflectionTestUtils.setField(domain, "id", id);

        TestEvent ev = TestEvent.builder()
            .streamId(String.valueOf(id))
            .streamType(domain.getClass().getName())
            .build();

        domain.raiseDomainEvent(ev);
        aggregate.aggregate(domain);

        assertEquals(1, a.count.get());
        assertEquals(1, b.count.get());
        assertEquals(0, domain.getUncommittedEvents().size());
    }

    @Test
    void failingHandlerDoesNotPreventOthers() {
        FailingHandler failing = new FailingHandler();
        HandlerA ok = new HandlerA();
        ApplicationContext context = mock(ApplicationContext.class);
        when(context.getBeansOfType(Object.class)).thenReturn(Map.of("f", failing, "ok", ok));

        DataPersistent<TestDomain> persistent = domain -> domain;
        TestAggregate aggregate = new TestAggregate(persistent, context);

        TestDomain domain = new TestDomain();
        UUID id = UUID.randomUUID();
        ReflectionTestUtils.setField(domain, "id", id);

        TestEvent ev = TestEvent.builder()
            .streamId(String.valueOf(id))
            .streamType(domain.getClass().getName())
            .build();

        domain.raiseDomainEvent(ev);
        aggregate.aggregate(domain);

        // failing handler was invoked, but its exception should not stop other handlers
        assertEquals(1, failing.count.get());
        assertEquals(1, ok.count.get());
        assertEquals(0, domain.getUncommittedEvents().size());
    }

    @Test
    void reactiveAggregateInvokesHandlers() {
        HandlerA a = new HandlerA();
        ApplicationContext context = mock(ApplicationContext.class);
        when(context.getBeansOfType(Object.class)).thenReturn(Map.of("a", a));

        DataPersistent<TestDomain> persistent = domain -> domain;
        TestAggregate aggregate = new TestAggregate(persistent, context);

        TestDomain domain = new TestDomain();
        UUID id = UUID.randomUUID();
        ReflectionTestUtils.setField(domain, "id", id);

        TestEvent ev = TestEvent.builder()
            .streamId(String.valueOf(id))
            .streamType(domain.getClass().getName())
            .build();

        domain.raiseDomainEvent(ev);
        aggregate.reactiveAggregate(domain).block();

        assertEquals(1, a.count.get());
        assertEquals(0, domain.getUncommittedEvents().size());
    }

    @Test
    void beanFactoryMethodReturningConsumerIsRegistered() {
        // Bean-style handler: method annotated with @HandleDomainEvent and returns Consumer<T>
        class BeanFactory {
            final AtomicInteger count = new AtomicInteger();

            @HandleDomainEvent(messageType = TestEvent.class)
            public java.util.function.Consumer<TestEvent> topUpTransactionFailedEventHandler1() {
                return ev -> count.incrementAndGet();
            }
        }

        BeanFactory beanFactory = new BeanFactory();
        ApplicationContext context = mock(ApplicationContext.class);
        when(context.getBeansOfType(Object.class)).thenReturn(Map.of("beanFactory", beanFactory));

        DataPersistent<TestDomain> persistent = domain -> domain;
        TestAggregate aggregate = new TestAggregate(persistent, context);

        TestDomain domain = new TestDomain();
        UUID id = UUID.randomUUID();
        ReflectionTestUtils.setField(domain, "id", id);

        TestEvent ev = TestEvent.builder()
            .streamId(String.valueOf(id))
            .streamType(domain.getClass().getName())
            .build();

        domain.raiseDomainEvent(ev);
        aggregate.aggregate(domain);

        assertEquals(1, beanFactory.count.get());
        assertEquals(0, domain.getUncommittedEvents().size());
    }
}

