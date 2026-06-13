package com.codeicator.domain;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.aop.support.AopUtils;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
public abstract class Aggregate<T extends Aggregate.Domain,IDType> {
    private static final Logger log = LoggerFactory.getLogger(Aggregate.class);

    @Getter    protected final DataPersistent<T> dataPersistent;
    private static volatile boolean handlersInitialized = false;



    public Aggregate(DataPersistent<T> persistent, ApplicationContext context) {
        this.dataPersistent = Objects.requireNonNull(persistent,
            "DataPersistent cannot be null");
        this.context = context;
        // Initialize TransactionTemplate if possible. Keep it optional to remain
        // usable in lightweight test contexts without a transaction manager.
        TransactionTemplate tx = null;
        try {
            tx = context.getBean(TransactionTemplate.class);
        } catch (Exception e) {
            try {
                PlatformTransactionManager ptm = context.getBean(PlatformTransactionManager.class);
                if (ptm != null) {
                    tx = new TransactionTemplate(ptm);
                }
            } catch (Exception ignored) {
                // no transaction support in context
            }
        }
        this.transactionTemplate = tx;
    }

    private static Object getIDFieldValue(Object instance) throws Exception {
        // 1. Get the field from the class definition
        Class<?> clazz = instance.getClass();
        Field field = clazz.getDeclaredField("id");

        // 2. Bypass private modifier checks
        field.setAccessible(true);

        // 3. Extract and return the value from the specific instance
        var id = field.get(instance);
        if (id == null) {
            throw new IllegalStateException("Domain id field is null");
        }

        // We cannot use `instanceof` with the generic type parameter IDType because
        // of type erasure. Instead check the runtime type against the declared
        // field type and then perform an unchecked cast.
        Class<?> declared = field.getType();
        if (!declared.isAssignableFrom(id.getClass())) {
            throw new IllegalStateException(String.format(
                "Domain id field is not of expected type. Declared=%s, Actual=%s",
                declared.getName(), id.getClass().getName()));
        }

        return id;
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
            Objects.requireNonNull(event, "DomainEvent cannot be null");
            try {
                lock.lock();

                // Validate event metadata instead of overriding it.
                // Rules:
                //  - streamType must exactly match the domain class name
                //  - streamId must equal the domain id (stringified)
                //  - version must match the current domain version
                String expectedStreamType = this.getClass().getName();
                String evStreamType = event.getStreamType();
                if (evStreamType == null || !evStreamType.equals(expectedStreamType)) {
                    throw new IllegalArgumentException(String.format(
                        "Event streamType '%s' is not compatible with domain '%s'",
                        evStreamType, expectedStreamType));
                }
                var id = getIDFieldValue(this);
                if (id == null) {
                    throw new IllegalStateException("Domain id is not set; cannot validate event streamId");
                }

                String evStreamId = event.getStreamId();
                String expectedStreamId = String.valueOf(id);
                if (evStreamId == null || !evStreamId.equals(expectedStreamId)) {
                    throw new IllegalArgumentException(String.format(
                        "Event streamId '%s' is not compatible with domain id '%s'",
                        evStreamId, expectedStreamId));
                }

                // Do not validate event.version against domain.version here; versions may differ by design.
                // The domain's version will be incremented when the event is accepted.

                // If validation passed, accept the event instance as-is (no metadata overriding).
                domainEvents.add(event);
                this.version++;

            } catch (RuntimeException e) {
                // Let runtime exceptions (e.g. IllegalArgumentException, IllegalStateException)
                // propagate directly so callers/tests can assert on them.
                throw e;
            } catch (Exception e) {
                // Wrap checked/reflective exceptions to avoid leaking implementation details
                throw new RuntimeException(e);
            } finally {
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

        /**
         * Find a builder setter method by property name that can accept the provided value.
         * Returns null if none found.
         */
        private static java.lang.reflect.Method findBuilderSetter(Class<?> builderClass, String propName, Object value) {
            for (java.lang.reflect.Method m : builderClass.getMethods()) {
                if (!m.getName().equals(propName) || m.getParameterCount() != 1) continue;
                Class<?> param = m.getParameterTypes()[0];
                if (value == null) {
                    if (!param.isPrimitive()) return m; // accept reference types for null
                    else continue; // cannot pass null to primitive
                }
                Class<?> valClass = value.getClass();
                if (param.isPrimitive()) {
                    // map primitives to wrappers
                    Class<?> wrapper = primitiveToWrapper(param);
                    if (wrapper.isAssignableFrom(valClass)) return m;
                } else {
                    if (param.isAssignableFrom(valClass)) return m;
                }
            }
            return null;
        }

        private static Class<?> primitiveToWrapper(Class<?> primitive) {
            if (primitive == int.class) return Integer.class;
            if (primitive == long.class) return Long.class;
            if (primitive == boolean.class) return Boolean.class;
            if (primitive == byte.class) return Byte.class;
            if (primitive == char.class) return Character.class;
            if (primitive == float.class) return Float.class;
            if (primitive == double.class) return Double.class;
            if (primitive == short.class) return Short.class;
            return primitive;
        }


    }
    private void ensureHandlersInitialized() {
        if (!handlersInitialized) {
            synchronized (Aggregate.class) {
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

    // Optional TransactionTemplate used to ensure aggregate operations run inside a
    // transactional boundary even when invoked from non-transactional callers.
    protected final TransactionTemplate transactionTemplate;

    @Getter
    protected static final ConcurrentHashMap<String,List<DomainEventHandler>> handlersMap=new ConcurrentHashMap<>();

    protected void registerHandlers() throws ClassNotFoundException, NoSuchMethodException {
        var beans = context.getBeansOfType(Object.class);

        for (var entry : beans.entrySet()) {
            Object handlerInstance = entry.getValue();
            Class<?> targetClass = AopUtils.getTargetClass(handlerInstance);

            for (Method method : targetClass.getMethods()) {
                HandleDomainEvent annotation = method.getAnnotation(HandleDomainEvent.class);
                if (annotation == null) {
                    continue;
                }
                if (method.getParameterCount() != 1) {
                    log.warn("Skipping handler method {}.{}: expected single parameter",
                            targetClass.getName(), method.getName());
                    continue;
                }
                ReflectionUtils.makeAccessible(method);

                Consumer<Object> func = message -> {
                    try {
                        method.invoke(handlerInstance, message);
                    } catch (Exception e) {
                        log.error("Error invoking handler {}.{}", targetClass.getName(), method.getName(), e);
                    }
                };

                var messageHandler = new DomainEventHandler(func, annotation.messageType());

                getHandlersMap().computeIfAbsent(annotation.messageType().getName(), key -> new ArrayList<>())
                    .add(messageHandler);
            }
        }
    }

    public final T aggregate(T domain) {
        Objects.requireNonNull(domain, "Domain cannot be null");
        ensureHandlersInitialized();
        return runInTransaction(() -> doAggregate(domain));
    }

    public final Mono<T> reactiveAggregate(T domain) {
        Objects.requireNonNull(domain, "Domain cannot be null");
        ensureHandlersInitialized();
        return Mono.fromSupplier(() -> runInTransaction(() -> doAggregate(domain)))
            .subscribeOn(Schedulers.boundedElastic());
    }

    // Core aggregate logic extracted to be callable inside or outside a transaction
    private T doAggregate(T domain) {
        T updatedDomain;
        try {
            domain.getLock().lock();

            // Just persist - outbox pattern handles event publishing
            updatedDomain = this.getDataPersistent().persist(domain);
            log.debug("Successfully persist domain {} domain events", updatedDomain);

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

    private T runInTransaction(Supplier<T> supplier) {
        if (this.transactionTemplate != null) {
            return this.transactionTemplate.execute(status -> supplier.get());
        }
        return supplier.get();
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
