package com.codeicator.infrastructure.persistence;

import com.codeicator.domain.*;
import com.codeicator.infrastructure.OutboxEvent;
import com.codeicator.infrastructure.OutboxStore;
import com.codeicator.messages.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
@Slf4j
public class JpaDataPersistentWithEventStore<T extends Aggregate.Domain>
    implements DataPersistent<T> {

    private final JpaRepository<T,?> aggregateRepository;
    private final ObjectMapper objectMapper; // ✅ Add ObjectMapper
    private final OutboxStore outboxStore;
    private final String topic;

    public JpaDataPersistentWithEventStore(
        JpaRepository<T,?> aggregateRepository,
        ObjectMapper objectMapper,
        OutboxStore outboxStore) {  // ✅ Inject ObjectMapper
        this(aggregateRepository, objectMapper, outboxStore, null);
    }

    /**
     * @param topic the service's event topic ({@code bus.event.destination}); stamped onto
     *     every outbox row so any poller of a shared outbox table can relay the raw payload to the
     *     correct topic without deserializing it
     */
    public JpaDataPersistentWithEventStore(
        JpaRepository<T,?> aggregateRepository,
        ObjectMapper objectMapper,
        OutboxStore outboxStore,
        String topic) {
        this.aggregateRepository = Objects.requireNonNull(aggregateRepository);
        this.objectMapper = Objects.requireNonNull(objectMapper);
        this.outboxStore = Objects.requireNonNull(outboxStore);
        this.topic = topic;
    }

    /**
     * Outbox topic for an event: the configured {@code topic} when set, otherwise derived from the
     * aggregate and event simple class names — {@code <Aggregate>.<Event>} (e.g. {@code
     * Tenant.TenantCreatedEvent}).
     */
    private String resolveTopic(T domain, Event event) {
        if (topic != null) {
            return topic;
        }
        return domain.getClass().getSimpleName() + "." + event.getClass().getSimpleName();
    }

    @Override
    @Transactional
    public T persist(T domain) throws OptimisticLockException {
        Objects.requireNonNull(domain, "Domain cannot be null");

        try {
            List<Event> uncommittedEvents = domain.getUncommittedEvents();

            log.debug("Persisting domain with {} uncommitted events",
                uncommittedEvents.size());

            T savedDomain;
            try {
                savedDomain = aggregateRepository.save(domain);
                log.debug("Domain state persisted successfully. New version: {}",
                    savedDomain.getVersion());
            } catch (OptimisticLockException e) {
                log.warn("Optimistic lock failure");
                throw e;
            }

            if (!uncommittedEvents.isEmpty()) {
                List<OutboxEvent> outboxEvents = uncommittedEvents.stream()
                    .map(event -> OutboxEvent.from(event, objectMapper, resolveTopic(domain, event)))
                    .collect(Collectors.toList());

                outboxStore.saveAll(outboxEvents);
                log.debug("Saved {} events to outbox", outboxEvents.size());
            }

            log.info("Successfully persisted domain and {} events in single transaction",
                uncommittedEvents.size());

            return savedDomain;

        } catch (OptimisticLockException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to persist domain: {}", e.getMessage(), e);
            throw new DataPersistenceException(
                "Failed to persist domain and events", e);
        }
    }
}
