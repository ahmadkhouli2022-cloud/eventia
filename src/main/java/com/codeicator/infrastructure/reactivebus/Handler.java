package com.codeicator.infrastructure.reactivebus;
import java.util.function.Consumer;


public record Handler(Consumer<Object> Processor, Class<?> Type, String Topic) {

}
