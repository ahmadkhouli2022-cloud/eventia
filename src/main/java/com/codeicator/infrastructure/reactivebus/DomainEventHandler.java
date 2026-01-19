package com.codeicator.infrastructure.reactivebus;

import java.util.function.Consumer;

public record DomainEventHandler(Consumer<Object> Processor, Class<?> Type) {
}
