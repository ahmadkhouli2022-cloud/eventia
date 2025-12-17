package com.codeicator.domain;

/**
 * Exception thrown when optimistic lock conflict is detected.
 *
 * This happens when:
 * - Domain version in memory differs from version in storage
 * - Another process modified the aggregate concurrently
 * - Indicates concurrent modification conflict
 *
 * Clients should:
 * 1. Reload the aggregate from storage
 * 2. Reapply their changes
 * 3. Retry the operation
 */
public class OptimisticLockException extends RuntimeException {

    private final long expectedVersion;
    private final long actualVersion;
    private final String aggregateId;

    public OptimisticLockException(String aggregateId, long expectedVersion, long actualVersion) {
        super(String.format(
            "Optimistic lock conflict for aggregate %s: expected version %d but found %d",
            aggregateId, expectedVersion, actualVersion
        ));
        this.aggregateId = aggregateId;
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }

    public long getExpectedVersion() {
        return expectedVersion;
    }

    public long getActualVersion() {
        return actualVersion;
    }

    public String getAggregateId() {
        return aggregateId;
    }
}

