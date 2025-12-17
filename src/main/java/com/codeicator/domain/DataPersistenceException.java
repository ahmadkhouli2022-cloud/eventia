package com.codeicator.domain;

public class DataPersistenceException extends RuntimeException {
    public DataPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
