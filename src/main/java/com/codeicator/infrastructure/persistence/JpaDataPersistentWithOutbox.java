package com.codeicator.infrastructure.persistence;

import com.codeicator.domain.*;
import com.codeicator.messages.Event;
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

    /**
     * Repository for persisting aggregate state.
     * Must support optimistic locking.
     */
    private final JpaRepository<T,?> aggregateRepository;

    /**
     * Store for persisting outbox events.
     * Handles all outbox-related operations.
     */
    private final OutboxStore outboxStore;

    public JpaDataPersistentWithOutbox(
        JpaRepository<T,?> aggregateRepository,
        OutboxStore outboxStore) {
        this.aggregateRepository = Objects.requireNonNull(aggregateRepository,
            "AggregateRepository cannot be null");
        this.outboxStore = Objects.requireNonNull(outboxStore,
            "OutboxStore cannot be null");
    }

    /**
     * Persist domain state and events atomically using Outbox pattern.
     *
     * CRITICAL: This method runs in a SINGLE database transaction.
     * If any step fails, the entire transaction rolls back.
     *
     * Steps:
     * 1. Extract uncommitted events from domain
     * 2. Save domain state to aggregates table (with optimistic locking)
     * 3. Convert domain events to OutboxEvent objects
     * 4. Save outbox events to outbox table
     * 5. Commit transaction (Spring @Transactional handles this)
     *
     * Guarantees:
     * - Domain state and events saved atomically
     * - Optimistic locking prevents lost updates
     * - Events cannot be lost if domain save succeeds
     * - Transaction rollback on any failure
     *
     * @param domain The domain object to persist
     * @return The persisted domain with updated version
     * @throws OptimisticLockException if version conflict detected
     * @throws DataPersistenceException if persistence fails
     */
    @Override
    @Transactional
    public T persist(T domain) throws OptimisticLockException {
        Objects.requireNonNull(domain, "Domain cannot be null");

        try {
            // Step 1: Extract uncommitted events BEFORE saving
            // This ensures we capture all events even if domain is modified
            List<Event> uncommittedEvents = domain.getUncommittedEvents();

            log.debug("Persisting domain with {} uncommitted events",
                uncommittedEvents.size());

            // Step 2: Save domain state with optimistic locking
            // Repository should implement version checking:
            // UPDATE aggregates SET version = ?, data = ?
            // WHERE id = ? AND version = (? - 1)
            T savedDomain;
            try {
                savedDomain = aggregateRepository.save(domain);

                log.debug("Domain state persisted successfully. New version: {}",
                    savedDomain.getVersion());

            } catch (OptimisticLockException e) {
                log.warn("Optimistic lock failure for domain. " +
                        "Current version: {}, Expected version: {}",
                    e.getActualVersion(), e.getExpectedVersion());
                throw e;  // Let caller handle retry
            }

            // Step 3 & 4: Convert events to OutboxEvent and save to outbox table
            if (!uncommittedEvents.isEmpty()) {
                List<OutboxEvent> outboxEvents = uncommittedEvents.stream()
                    .map(OutboxEvent::from)
                    .collect(Collectors.toList());

                outboxStore.saveAll(outboxEvents);

                log.debug("Saved {} events to outbox", outboxEvents.size());
            }

            // Transaction will commit here (via @Transactional)
            // At this point:
            // ✓ Domain state saved to database
            // ✓ Events saved to outbox table
            // ✓ Both in same transaction (all or nothing)

            log.info("Successfully persisted domain and {} events in single transaction",
                uncommittedEvents.size());

            return savedDomain;

        } catch (OptimisticLockException e) {
            // Expected exception - rethrow for caller to handle
            throw e;

        } catch (Exception e) {
            // Unexpected exception - transaction will rollback
            log.error("Failed to persist domain: {}", e.getMessage(), e);
            throw new DataPersistenceException(
                "Failed to persist domain and events", e);
        }
    }

}
