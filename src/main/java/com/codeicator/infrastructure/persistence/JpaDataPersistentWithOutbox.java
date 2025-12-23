package com.codeicator.infrastructure.persistence;

import com.codeicator.domain.*;
import com.codeicator.messages.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
@Service
@Slf4j
public class JpaDataPersistentWithOutbox<T extends Aggregate.Domain>
    implements DataPersistent<T> {

    private final JpaRepository<T,?> aggregateRepository;
    private final OutboxStore outboxStore;
    private final ObjectMapper objectMapper; // ✅ Add ObjectMapper

    public JpaDataPersistentWithOutbox(
        JpaRepository<T,?> aggregateRepository,
        OutboxStore outboxStore,
        ObjectMapper objectMapper) {  // ✅ Inject ObjectMapper
        this.aggregateRepository = Objects.requireNonNull(aggregateRepository);
        this.outboxStore = Objects.requireNonNull(outboxStore);
        this.objectMapper = Objects.requireNonNull(objectMapper);
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
                    .map(event -> OutboxEvent.from(event, objectMapper)) // ✅ Pass ObjectMapper
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
