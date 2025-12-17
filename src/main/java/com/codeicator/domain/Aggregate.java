package com.codeicator.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.codeicator.messages.Event;

import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
public abstract class Aggregate<T extends Aggregate.Domain> {
    private static final Logger log = LoggerFactory.getLogger(Aggregate.class);

    protected final DataPersistent<T> dataPersistent;

    protected final EventPublisher eventPublisher;


    public Aggregate(DataPersistent<T> persistent, EventPublisher publisher) {
        this.dataPersistent = Objects.requireNonNull(persistent,
            "DataPersistent cannot be null");
        this.eventPublisher = Objects.requireNonNull(publisher,
            "EventPublisher cannot be null");
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
        protected final void raiseDomainEvent(Event event) {
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

    public final T aggregate(T domain) {
        Objects.requireNonNull(domain, "Domain cannot be null");

        List<Event> eventsToPublish = new ArrayList<>();
        T updatedDomain;
        try {
            domain.getLock().lock();
            updatedDomain = this.dataPersistent.persist(domain);
            // Keep copy before clearing to prevent race condition
            eventsToPublish.addAll(domain.getUncommittedEvents());
            domain.getUncommittedEvents().clear();

            log.debug("Domain state persisted with {} uncommitted events",
                eventsToPublish.size());
        } finally {
            domain.getLock().unlock();
        }

        try {
            eventsToPublish.forEach(event -> {
                log.debug("Publishing event: {} (version: {})",
                    event.getClass().getSimpleName(), event.getVersion());
                this.eventPublisher.publish(event);
            });
            markEventsPublished(domain);

            log.info("Successfully published {} events", eventsToPublish.size());

        } catch (Exception e) {
            log.warn("Event publishing failed for {} events: {}",
                eventsToPublish.size(), e.getMessage(), e);

            // Re-queue events for retry if publishing fails
            try {
                domain.getLock().lock();
                domain.getUncommittedEvents().addAll(eventsToPublish);

                log.warn("Events re-queued for retry. Current queue size: {}",
                    domain.getUncommittedEvents().size());

            } finally {
                domain.getLock().unlock();
            }
            throw new EventPublishingException(
                "Event publishing failed for " + eventsToPublish.size() + " events",
                e
            );
        }

        return Objects.requireNonNull(updatedDomain);
    }

    public final Mono<T> reactiveAggregate(T domain) {
        return Mono.fromSupplier(() -> {
            Objects.requireNonNull(domain, "Domain cannot be null");

            List<Event> eventsToPublish = new ArrayList<>();
            T updatedDomain;
            try {
                domain.getLock().lock();
                updatedDomain = this.dataPersistent.persist(domain);
                // Keep copy before clearing to prevent race condition
                eventsToPublish.addAll(domain.getUncommittedEvents());
                domain.getUncommittedEvents().clear();

                log.debug("Domain state persisted with {} uncommitted events",
                    eventsToPublish.size());
            } finally {
                domain.getLock().unlock();
            }

            try {
                eventsToPublish.forEach(event -> {
                    log.debug("Publishing event: {} (version: {})",
                        event.getClass().getSimpleName(), event.getVersion());
                    this.eventPublisher.publish(event);
                });
                markEventsPublished(domain);

                log.info("Successfully published {} events", eventsToPublish.size());

            } catch (Exception e) {
                log.warn("Event publishing failed for {} events: {}",
                    eventsToPublish.size(), e.getMessage(), e);

                // Re-queue events for retry if publishing fails
                try {
                    domain.getLock().lock();
                    domain.getUncommittedEvents().addAll(eventsToPublish);

                    log.warn("Events re-queued for retry. Current queue size: {}",
                        domain.getUncommittedEvents().size());

                } finally {
                    domain.getLock().unlock();
                }
                throw new EventPublishingException(
                    "Event publishing failed for " + eventsToPublish.size() + " events",
                    e
                );
            }

            return Objects.requireNonNull(updatedDomain);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    protected final void markEventsPublished(Domain domain) {
        try {
            domain.getLock().lock();
            domain.markEventsAsCommitted();
        } finally {
            domain.getLock().unlock();
        }
    }

    public final void raiseEvent(Event event) {
        try {
            Objects.requireNonNull(event, "Event cannot be null");
            this.eventPublisher.publish(event);
            log.debug("Event published: {}", event.getClass().getSimpleName());
        } catch (Exception e) {
            log.error("Failed to publish event: {}", e.getMessage(), e);
            throw new EventPublishingException("Event publishing failed", e);
        }
    }

    public final Mono<Void> raiseEventReactive(Event event) {
        Objects.requireNonNull(event, "Event cannot be null");
        return Mono.fromRunnable(() -> this.eventPublisher.publish(event))
            .doOnSuccess(v -> log.debug("Event published reactively: {}",
                event.getClass().getSimpleName()))
            .onErrorResume(e -> {
                log.error("Failed to publish event reactively: {}", e.getMessage(), e);
                return Mono.error(new EventPublishingException("Event publishing failed", e));
            })
            .then();
    }




}
