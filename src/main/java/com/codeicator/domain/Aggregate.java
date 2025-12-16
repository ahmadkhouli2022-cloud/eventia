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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public abstract class Aggregate<T extends Aggregate.Domain> {

    @SuperBuilder(toBuilder = true)
    @NoArgsConstructor
    public abstract static class Domain {
        @JsonIgnore
        protected final transient Lock lock=new ReentrantLock();

        @JsonIgnore
        private final transient List<Event> domainEvents = new ArrayList<>();

        @JsonIgnore
        protected final void raiseDomainEvent(Event event) {
            try {
                lock.lock();
                domainEvents.add(event);
            }finally {
                lock.unlock();
            }
        }

    }

    public final T aggregate(T domain) {
        List<Event> eventsToPublish=new ArrayList<>();
        T updatedDomain;
        try {
            domain.lock.lock();
            updatedDomain= this.dataPersistent.persist(domain);
            eventsToPublish.addAll(((Domain) domain).domainEvents);
            ((Domain) domain).domainEvents.clear();
        }finally {
            domain.lock.unlock();
        }
        eventsToPublish.forEach(this.eventPublisher::publish);
        return Objects.requireNonNull(updatedDomain);
    }

    public final Mono<T> reactiveAggregate(T domain) {
        return Mono.fromSupplier(() -> {
            List<Event> eventsToPublish=new ArrayList<>();
            T updatedDomain;
            try {
                domain.lock.lock();
                updatedDomain= this.dataPersistent.persist(domain);
                eventsToPublish.addAll(((Domain) domain).domainEvents);
                ((Domain) domain).domainEvents.clear();
            }finally {
                domain.lock.unlock();
            }
            eventsToPublish.forEach(this.eventPublisher::publish);
            return Objects.requireNonNull(updatedDomain);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public final void raiseEvent(Event event) {
        this.eventPublisher.publish(event);
    }

    public final Mono<Void> raiseEventReactive(Event event) {
        return Mono.fromRunnable(() -> this.eventPublisher.publish(event));
    }

    private DataPersistent<T> dataPersistent;

    private EventPublisher eventPublisher;

    @Lazy
    @Autowired
    public void setDataPersistent(DataPersistent<T> persistent) {
        this.dataPersistent = persistent;
    }

    @Lazy
    @Autowired
    public void setEventPublisher(EventPublisher publisher) {
        this.eventPublisher = publisher;
    }

    protected Aggregate(DataPersistent<T> persistent, EventPublisher publisher) {
        this.dataPersistent = persistent;
        this.eventPublisher = publisher;
    }
    protected Aggregate(){}

}
