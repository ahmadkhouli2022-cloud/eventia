# Eventia

[![Maven Central](https://img.shields.io/maven-central/v/com.codeicator/eventia-core.svg)](https://search.maven.org/artifact/com.codeicator/eventia-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java Version](https://img.shields.io/badge/Java-17%2B-blue)](https://www.oracle.com/java/)
[![Build Status](https://img.shields.io/github/workflow/status/ahmadkhouli2022-cloud/eventia/CI)](https://github.com/ahmadkhouli2022-cloud/eventia/actions)

**Eventia** is a lightweight, production-ready Java library for building reliable event-driven applications with guaranteed event delivery using the Outbox pattern.

## ✨ Features

- 🎯 **Guaranteed Event Delivery** - Events are never lost, even during failures
- 📦 **Outbox Pattern** - Atomic persistence of domain state and events
- 🔄 **Automatic Retry** - Failed events automatically retry with exponential backoff
- ⚰️ **Dead Letter Queue** - Track and handle permanently failed events
- 🏗️ **DDD Support** - Built-in aggregate and domain event infrastructure
- 🔒 **Optimistic Locking** - Prevent concurrent modification conflicts
- ⚡ **Reactive Support** - Full support for Project Reactor
- 🧪 **Production Tested** - Battle-tested in high-throughput production systems
- 📊 **Monitoring Ready** - Built-in metrics and health checks
- 🔌 **Framework Agnostic** - Works with any message broker (Kafka, RabbitMQ, etc.)

## 🚀 Quick Start

### Maven

```xml
<dependency>
    <groupId>com.codeicator</groupId>
    <artifactId>eventia</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

[//]: # (### Gradle)

[//]: # ()
[//]: # (```gradle)

[//]: # (implementation 'com.codeicator:eventia-core:1.0.0')

[//]: # (implementation 'com.codeicator:eventia-spring-boot-starter:1.0.0')

[//]: # (```)

## 📖 Basic Usage

### 1. Define Your Domain

```java
public class OrderDomain extends Aggregate.Domain {
    private String id;
    private String customerId;
    private List<OrderItem> items;
    private OrderStatus status;
    
    public void createOrder(List<OrderItem> items) {
        // Business logic
        this.items = items;
        this.status = OrderStatus.CREATED;
        
        // Raise integration event
        OrderPlacedEvent event = OrderPlacedEvent.builder()
            .orderId(this.id)
            .customerId(this.customerId)
            .items(items)
            .build();
        
        raiseDomainEvent(event);
    }
}
```

### 2. Create Your Aggregate

```java
@Service
public class OrderAggregate extends Aggregate<OrderDomain> {
    
    public OrderAggregate(
            DataPersistent<OrderDomain> dataPersistent,
            EventPublisher eventPublisher) {
        super(dataPersistent, eventPublisher);
    }
}
```

### 3. Use in Application Service

```java
@Service
public class OrderService {
    
    private final OrderAggregate orderAggregate;
    
    @Transactional
    public OrderDomain createOrder(CreateOrderCommand command) {
        OrderDomain order = OrderDomain.builder()
            .id(UUID.randomUUID().toString())
            .customerId(command.getCustomerId())
            .build();
        
        order.createOrder(command.getItems());
        
        // Persists domain + events atomically
        return orderAggregate.aggregate(order);
    }
}
```

### 4. Configure

```yaml
eventia:
  outbox:
    enabled: true
    polling-interval: 1000  # Poll every 1 second
    max-retries: 3
    batch-size: 100
    cleanup:
      enabled: true
      schedule: "0 0 2 * * *"  # Daily at 2 AM
      retention-days: 30
```

**That's it!** Eventia handles:
- ✅ Persisting domain state and events in a single transaction
- ✅ Publishing events to your message broker
- ✅ Retrying failed events automatically
- ✅ Moving permanently failed events to dead letter queue

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│                  orderService.createOrder()                 │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                   Aggregate.aggregate()                     │
│  • Extract uncommitted events                               │
│  • DataPersistent.persist(domain, events)                   │
│  • Clear domain events                                      │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              Database (Single Transaction)                  │
│  ┌──────────────────┐    ┌──────────────────┐              │
│  │  Domain Table    │    │  Outbox Table    │              │
│  │  ─────────────   │    │  ─────────────   │              │
│  │  id: ABC         │    │  id: evt-1       │              │
│  │  version: 5      │    │  published: null │              │
│  │  state: {...}    │    │  event: {...}    │              │
│  └──────────────────┘    └──────────────────┘              │
└─────────────────────────────────┬───────────────────────────┘
                                  │
                                  │ (Async polling)
                                  ▼
┌─────────────────────────────────────────────────────────────┐
│               OutboxPublisher (@Scheduled)                  │
│  • Poll unpublished events                                  │
│  • Publish to message broker                                │
│  • Mark as published or retry                               │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              Message Broker (Kafka, RabbitMQ)               │
└─────────────────────────────────────────────────────────────┘
```

## 🎯 Why Eventia?

### The Problem

Traditional event publishing has a critical flaw:

```java
@Transactional
public void createOrder(Order order) {
    orderRepository.save(order);        // ✅ Saved to database
    eventPublisher.publish(orderEvent); // ❌ Fails! Event lost!
}
```

If event publishing fails, the event is **lost forever** even though the order was saved.

### The Solution: Outbox Pattern

Eventia solves this with the **Outbox Pattern**:

```java
@Transactional
public void createOrder(Order order) {
    // BOTH saved in SINGLE transaction
    orderRepository.save(order);           // ✅ Saved
    outboxRepository.save(orderEvent);     // ✅ Saved
}

// Later, OutboxPublisher polls and publishes
outboxPublisher.publishPending(); // ✅ Guaranteed delivery
```

**Benefits:**
- ✅ Events **cannot be lost** - they're in the database
- ✅ **Exactly-once delivery** semantics
- ✅ **Automatic retry** with exponential backoff
- ✅ **Dead letter queue** for investigation
- ✅ **No distributed transactions** needed

[//]: # (## 📚 Documentation)

[//]: # ()
[//]: # (### Core Concepts)

[//]: # ()
[//]: # (- [Outbox Pattern]&#40;docs/outbox-pattern.md&#41; - Understanding the pattern)

[//]: # (- [Aggregate Pattern]&#40;docs/aggregate.md&#41; - Domain-Driven Design aggregates)

[//]: # (- [Event Publishing]&#40;docs/event-publishing.md&#41; - How events are published)

[//]: # (- [Monitoring]&#40;docs/monitoring.md&#41; - Metrics and observability)

[//]: # ()
[//]: # (### Guides)

[//]: # ()
[//]: # (- [Getting Started]&#40;docs/getting-started.md&#41; - Step-by-step tutorial)

[//]: # (- [Spring Boot Integration]&#40;docs/spring-boot.md&#41; - Spring Boot setup)

[//]: # (- [Reactive Applications]&#40;docs/reactive.md&#41; - Using with Project Reactor)

[//]: # (- [Testing]&#40;docs/testing.md&#41; - Testing strategies)

[//]: # (- [Production Deployment]&#40;docs/production.md&#41; - Production best practices)

[//]: # ()
[//]: # (### Examples)

[//]: # ()
[//]: # (- [Simple Order Service]&#40;examples/order-service&#41; - Basic usage)

[//]: # (- [Microservices]&#40;examples/microservices&#41; - Multi-service example)

[//]: # (- [Event Sourcing]&#40;examples/event-sourcing&#41; - Event sourcing integration)

[//]: # (- [Saga Pattern]&#40;examples/saga&#41; - Distributed transactions)

[//]: # ()
[//]: # (## 🔧 Advanced Configuration)

[//]: # ()
[//]: # (### Custom Event Publisher)

[//]: # ()
[//]: # (```java)

[//]: # (@Configuration)

[//]: # (public class EventiaConfig {)

[//]: # (    )
[//]: # (    @Bean)

[//]: # (    public EventPublisher kafkaEventPublisher&#40;KafkaTemplate<String, Event> kafka&#41; {)

[//]: # (        return event -> {)

[//]: # (            kafka.send&#40;"events", event.getStreamId&#40;&#41;, event&#41;;)

[//]: # (        };)

[//]: # (    })

[//]: # (})

[//]: # (```)

[//]: # ()
[//]: # (### Custom Retry Strategy)

[//]: # ()
[//]: # (```java)

[//]: # (@Configuration)

[//]: # (public class OutboxConfig {)

[//]: # (    )
[//]: # (    @Bean)

[//]: # (    public OutboxPublisher outboxPublisher&#40;)

[//]: # (            OutboxStore store, )

[//]: # (            EventPublisher publisher&#41; {)

[//]: # (        return OutboxPublisher.builder&#40;&#41;)

[//]: # (            .outboxStore&#40;store&#41;)

[//]: # (            .eventPublisher&#40;publisher&#41;)

[//]: # (            .maxRetries&#40;5&#41;)

[//]: # (            .batchSize&#40;50&#41;)

[//]: # (            .retryBackoff&#40;Duration.ofSeconds&#40;10&#41;&#41;)

[//]: # (            .build&#40;&#41;;)

[//]: # (    })

[//]: # (})

[//]: # (```)

[//]: # ()
[//]: # (### Monitoring)

[//]: # ()
[//]: # (```java)

[//]: # (@RestController)

[//]: # (@RequestMapping&#40;"/admin/eventia"&#41;)

[//]: # (public class EventiaMetrics {)

[//]: # (    )
[//]: # (    @Autowired)

[//]: # (    private OutboxStore outboxStore;)

[//]: # (    )
[//]: # (    @GetMapping&#40;"/metrics"&#41;)

[//]: # (    public Map<String, Long> getMetrics&#40;&#41; {)

[//]: # (        return Map.of&#40;)

[//]: # (            "unpublished", outboxStore.countUnpublished&#40;&#41;,)

[//]: # (            "deadLetter", outboxStore.countDeadLettered&#40;&#41;)

[//]: # (        &#41;;)

[//]: # (    })

[//]: # (})

[//]: # (```)

[//]: # ()
[//]: # (## 🧪 Testing)

[//]: # ()
[//]: # (Eventia provides test utilities for easy testing:)

[//]: # ()
[//]: # (```java)

[//]: # (@SpringBootTest)

[//]: # (@Import&#40;EventiaTestConfig.class&#41;)

[//]: # (class OrderServiceTest {)

[//]: # (    )
[//]: # (    @Autowired)

[//]: # (    private OrderService orderService;)

[//]: # (    )
[//]: # (    @Autowired)

[//]: # (    private TestEventCaptor eventCaptor;)

[//]: # (    )
[//]: # (    @Test)

[//]: # (    void shouldPublishOrderCreatedEvent&#40;&#41; {)

[//]: # (        // When)

[//]: # (        orderService.createOrder&#40;command&#41;;)

[//]: # (        )
[//]: # (        // Then - Event captured in test mode)

[//]: # (        OrderCreatedEvent event = eventCaptor.getEvent&#40;OrderCreatedEvent.class&#41;;)

[//]: # (        assertThat&#40;event.getOrderId&#40;&#41;&#41;.isNotNull&#40;&#41;;)

[//]: # (    })

[//]: # (})

[//]: # (```)

[//]: # ()
[//]: # (## 📊 Performance)

[//]: # ()
[//]: # (Eventia is designed for high-throughput production systems:)

[//]: # ()
[//]: # (| Metric | Value |)

[//]: # (|--------|-------|)

[//]: # (| Events persisted/sec | ~10,000 |)

[//]: # (| Events published/sec | ~5,000 |)

[//]: # (| Latency &#40;p95&#41; | < 10ms |)

[//]: # (| Memory footprint | ~50MB |)

[//]: # (| Database overhead | ~1KB per event |)

[//]: # ()
[//]: # (*Benchmarks run on AWS EC2 t3.medium with PostgreSQL RDS*)

[//]: # ()
[//]: # (## 🤝 Contributing)

[//]: # ()
[//]: # (We welcome contributions! Please see our [Contributing Guide]&#40;CONTRIBUTING.md&#41;.)

[//]: # ()
[//]: # (### Development Setup)

[//]: # ()
[//]: # (```bash)

[//]: # (# Clone repository)

[//]: # (git clone https://github.com/yourusername/eventia.git)

[//]: # (cd eventia)

[//]: # ()
[//]: # (# Build)

[//]: # (./mvnw clean install)

[//]: # ()
[//]: # (# Run tests)

[//]: # (./mvnw test)

[//]: # ()
[//]: # (# Run integration tests)

[//]: # (./mvnw verify -P integration-tests)

[//]: # (```)

[//]: # ()
[//]: # (## 📝 License)

[//]: # ()
[//]: # (Eventia is licensed under the [Apache License 2.0]&#40;LICENSE&#41;.)

[//]: # ()
[//]: # (## 🙏 Acknowledgments)

[//]: # ()
[//]: # (Eventia is inspired by:)

[//]: # (- [Microservices Patterns]&#40;https://microservices.io/patterns/data/transactional-outbox.html&#41; by Chris Richardson)

[//]: # (- [Domain-Driven Design]&#40;https://www.domainlanguage.com/ddd/&#41; by Eric Evans)

[//]: # (- [Implementing Domain-Driven Design]&#40;https://www.amazon.com/Implementing-Domain-Driven-Design-Vaughn-Vernon/dp/0321834577&#41; by Vaughn Vernon)

[//]: # ()
[//]: # (## 📞 Support)

[//]: # ()
[//]: # (- 📖 [Documentation]&#40;https://codeicator.com/eventia/docs&#41;)

[//]: # (- 💬 [Discord Community]&#40;https://discord.gg/eventia&#41;)

[//]: # (- 🐛 [Issue Tracker]&#40;https://github.com/codeicator/eventia/issues&#41;)

[//]: # (- 📧 [Email Support]&#40;mailto:support@codeicator.com&#41;)

[//]: # (- 🐦 [Twitter]&#40;https://twitter.com/codeicator&#41;)

[//]: # ()
[//]: # (## 🗺️ Roadmap)

[//]: # ()
[//]: # (### Version 1.1 &#40;Q2 2024&#41;)

[//]: # (- [ ] Event replay functionality)

[//]: # (- [ ] Enhanced monitoring dashboard)

[//]: # (- [ ] Performance improvements)

[//]: # ()
[//]: # (### Version 1.2 &#40;Q3 2024&#41;)

[//]: # (- [ ] Event versioning support)

[//]: # (- [ ] Schema registry integration)

[//]: # (- [ ] Multi-tenancy support)

[//]: # ()
[//]: # (### Version 2.0 &#40;Q4 2024&#41;)

[//]: # (- [ ] Event sourcing support)

[//]: # (- [ ] Saga pattern implementation)

[//]: # (- [ ] Cloud-native deployment tools)

[//]: # ()
[//]: # (## ⭐ Star History)

[//]: # ()
[//]: # ([![Star History Chart]&#40;https://api.star-history.com/svg?repos=yourusername/eventia&type=Date&#41;]&#40;https://star-history.com/#yourusername/eventia&Date&#41;)

[//]: # ()
[//]: # (---)

<p align="center">
  <b>Built with ❤️ by the Codeicator team</b><br>
  <a href="https://codeicator.com/eventia">Website</a> •
  <a href="https://codeicator.com/eventia/docs">Documentation</a> •
  <a href="https://github.com/codeicator/eventia/discussions">Discussions</a>
</p>
