package com.codeicator.domain;

/**
 * Repository interface for persisting aggregate state.
 *
 * IMPORTANT IMPLEMENTATION NOTES:
 *
 * 1. Thread Safety:
 *    - Implementations must be thread-safe
 *    - Use optimistic locking to detect conflicts
 *    - Throw OptimisticLockException on version mismatch
 *
 * 2. Transactions:
 *    - All persist operations must be transactional
 *    - Use database transactions to ensure atomicity
 *    - Rollback on any exception
 *
 * 3. Optimistic Locking:
 *    - Increment version on each save
 *    - Use version in WHERE clause for conflict detection
 *    - Example: UPDATE aggregates SET version = ? WHERE id = ? AND version = ?
 *    - Detect conflicts and throw OptimisticLockException
 *
 * 4. Error Handling:
 *    - Catch version conflicts and throw OptimisticLockException
 *    - Do NOT silently ignore conflicts
 *    - Let caller decide on retry strategy
 */
public interface DataPersistent<T extends Aggregate.Domain> {

    /**
     * Persist domain state with optimistic locking.
     *
     * Must:
     * - Be transactional (atomic)
     * - Update domain version on success
     * - Detect and reject concurrent modifications
     * - Preserve all uncommitted events context
     *
     * @param domain The domain object to persist
     * @return The persisted domain (with updated version)
     * @throws OptimisticLockException if version conflict detected
     */
    T persist(T domain) throws OptimisticLockException;
}
