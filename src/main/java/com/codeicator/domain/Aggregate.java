package com.codeicator.domain;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import com.codeicator.infrastructure.reactivebus.DomainEventHandler;
import com.codeicator.infrastructure.reactivebus.annotations.HandleDomainEvent;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.codeicator.messages.Event;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
public abstract class Aggregate<T extends Aggregate.Domain> {
    private static final Logger log = LoggerFactory.getLogger(Aggregate.class);

    @Getter    protected final DataPersistent<T> dataPersistent;
    private static volatile boolean handlersInitialized = false;



    public Aggregate(DataPersistent<T> persistent, ApplicationContext context) {
        this.dataPersistent = Objects.requireNonNull(persistent,
            "DataPersistent cannot be null");
        this.context = context;
    }


    @SuperBuilder(toBuilder = true)
    @NoArgsConstructor
    public abstract static class Domain {
        @JsonIgnore
        private final transient Lock lock=new ReentrantLock();

        @JsonIgnore
        private final transient List<Event> domainEvents = new ArrayList<>();

        @JsonIgnore
        private transient long version = 0;

        @JsonIgnore
        public final void raiseDomainEvent(Event event) {
            try {
                lock.lock();
                event.setVersion(this.version);
                domainEvents.add(event);
                this.version++;
            }finally {
                lock.unlock();
            }
        }

        @JsonIgnore
        public final long getVersion() {
            try {
                lock.lock();
                return this.version;
            } finally {
                lock.unlock();
            }
        }

        @JsonIgnore
        public final void setVersion(long version) {
            try {
                lock.lock();
                this.version = version;
            } finally {
                lock.unlock();
            }
        }

        @JsonIgnore
        public final List<Event> getUncommittedEvents() {
            try {
                lock.lock();
                return new ArrayList<>(domainEvents);
            } finally {
                lock.unlock();
            }
        }

        @JsonIgnore
        public final void markEventsAsCommitted() {
            try {
                lock.lock();
                domainEvents.clear();
            } finally {
                lock.unlock();
            }
        }

        /**
         * Provides safe access to the lock for synchronization.
         * Prevents unsafe casting and improves encapsulation.
         *
         * @return the lock for this domain
         */
        @JsonIgnore
        protected final Lock getLock() {
            return lock;
        }


    }
    private void ensureHandlersInitialized() {
        if (!handlersInitialized) {
            synchronized (this) {
                if (!handlersInitialized) {
                    try {
                        this.registerHandlers();
                    } catch (ClassNotFoundException | NoSuchMethodException e) {
                        log.error("Failed to register domain event handlers: {}", e.toString());
                    } finally {
                        handlersInitialized = true; // Mark as initialized regardless to prevent retry loops
                    }
                }
            }
        }
    }
    @Getter
    protected final ApplicationContext context;

    @Getter
    protected static final ConcurrentHashMap<String,List<DomainEventHandler>> handlersMap=new ConcurrentHashMap<>();

    protected void registerHandlers() throws ClassNotFoundException, NoSuchMethodException {
        var handlers = context.getBeansWithAnnotation(HandleDomainEvent.class);

        for (var entry : handlers.entrySet()) {
            Object handlerInstance = entry.getValue();
            Class<?> handlerClass = handlerInstance.getClass();

            String realClassName = handlerClass.getName().split("\\$")[0];
            Class<?> realClass = getClass().getClassLoader().loadClass(realClassName);

            Method method = realClass.getMethod(entry.getKey());
            HandleDomainEvent annotation = method.getAnnotation(HandleDomainEvent.class);

            @SuppressWarnings("unchecked")
            Consumer<Object> func = (Consumer<Object>) handlerInstance;

            var messageHandler = new DomainEventHandler(func, annotation.messageType());

            getHandlersMap().computeIfAbsent(annotation.messageType().getName(), key -> new ArrayList<>())
                .add(messageHandler);
        }
    }


    @Transactional
    public final T aggregate(T domain) {
        Objects.requireNonNull(domain, "Domain cannot be null");
        ensureHandlersInitialized();
        T updatedDomain;
        try {
            domain.getLock().lock();

            // Just persist - outbox pattern handles event publishing
            updatedDomain = this.getDataPersistent().persist(domain);
            log.debug("Successfully persist domain {} domain events",updatedDomain);

            for (Event msg : domain.getUncommittedEvents()) {
                var handlers = this.getHandlersMap().get(msg.getType());
                if (handlers != null) {
                    handlers.forEach(handler -> {
                        try {
                            log.debug("Processing event of type {} with handler {}", msg.getType(), handler);
                            handler.Processor().accept(msg);

                        } catch (Exception e) {
                            log.error("Error logging: handler " + handler.getClass() + " event processing", e);
                        }
                    });
                }
            }


            log.debug("Successfully handle {} domain events",
                domain.getUncommittedEvents().size());

            // Clear events after successful persist
            domain.markEventsAsCommitted();

        } finally {
            domain.getLock().unlock();
        }

        return Objects.requireNonNull(updatedDomain);
    }

    @Transactional
    public final Mono<T> reactiveAggregate(T domain) {
        return Mono.fromSupplier(() -> {
            Objects.requireNonNull(domain, "Domain cannot be null");
            ensureHandlersInitialized();

            T updatedDomain;
            try {
                domain.getLock().lock();
                updatedDomain = this.getDataPersistent().persist(domain);

                for (Event msg : domain.getUncommittedEvents()) {
                    var handlers = this.getHandlersMap().get(msg.getType());
                    if (handlers != null) {
                        handlers.forEach(handler -> {
                            try {
                                log.debug("Processing message of type {} with handler {}", msg.getType(), handler);
                                handler.Processor().accept(msg);

                            } catch (Exception e) {
                                log.error("Error logging: handler " + handler.getClass() + " message processing", e);
                            }
                        });
                    }
                }
                domain.markEventsAsCommitted();

            } finally {
                domain.getLock().unlock();
            }


            return Objects.requireNonNull(updatedDomain);
        }).subscribeOn(Schedulers.boundedElastic());
    }


//    public final void raiseEvent(Event event) {
//        try {
//            Objects.requireNonNull(event, "Event cannot be null");
//
//            log.debug("Event published: {}", event.getClass().getSimpleName());
//        } catch (Exception e) {
//            log.error("Failed to publish event: {}", e.getMessage(), e);
//            throw new EventPublishingException("Event publishing failed", e);
//        }
//    }
//
//    public final Mono<Void> raiseEventReactive(Event event) {
//        Objects.requireNonNull(event, "Event cannot be null");
//        return Mono.fromRunnable(() -> this.eventPublisher.publish(event))
//            .doOnSuccess(v -> log.debug("Event published reactively: {}",
//                event.getClass().getSimpleName()))
//            .onErrorResume(e -> {
//                log.error("Failed to publish event reactively: {}", e.getMessage(), e);
//                return Mono.error(new EventPublishingException("Event publishing failed", e));
//            })
//            .then();
//    }




}
