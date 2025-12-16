package com.codeicator.domain;

public interface DataPersistent<T extends Aggregate.Domain> {
    T persist(T domain);
}
