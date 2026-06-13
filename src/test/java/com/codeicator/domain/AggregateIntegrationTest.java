package com.codeicator.domain;

import com.codeicator.infrastructure.reactivebus.DomainEventHandler;
import com.codeicator.infrastructure.reactivebus.annotations.HandleDomainEvent;
import com.codeicator.messages.Event;
import jakarta.persistence.Id;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = AggregateIntegrationTest.TestApplication.class)
class AggregateIntegrationTest {

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApplication {
        @Bean
        TestHandler testHandler() {
            return new TestHandler();
        }

        @Bean
        DataPersistent<TestDomain> dataPersistent() {
            return domain -> domain;
        }
    }

    @SuperBuilder(toBuilder = true)
    private static class TestEvent extends Event {
    }

    private static class TestDomain extends Aggregate.Domain {
        @Id
        private UUID id;
    }

    private static class TestAggregate extends Aggregate<TestDomain,UUID> {
        TestAggregate(DataPersistent<TestDomain> persistent, ApplicationContext context) {
            super(persistent, context);
        }
    }

    private static class TestHandler {
        private final AtomicInteger count = new AtomicInteger(0);

        @HandleDomainEvent(messageType = TestEvent.class)
        public java.util.function.Consumer<TestEvent> topUpTransactionFailedEventHandler1() {
            return event -> count.incrementAndGet();
        }
    }

    @Autowired
    private ApplicationContext context;

    @Autowired
    private TestHandler handler;

    @Autowired
    private DataPersistent<TestDomain> persistent;

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
    void aggregateInvokesHandlersFromApplicationContext() {
        TestAggregate aggregate = new TestAggregate(persistent, context);
        TestDomain domain = new TestDomain();
        UUID id = UUID.randomUUID();
        ReflectionTestUtils.setField(domain, "id", id);
        TestEvent event = TestEvent.builder()
            .streamId(String.valueOf(id))
            .streamType(domain.getClass().getName())
            .correlationId("corr-1")
            .orderId(1)
            .build();

        domain.raiseDomainEvent(event);
        aggregate.aggregate(domain);

        assertEquals(1, handler.count.get());
    }
}
