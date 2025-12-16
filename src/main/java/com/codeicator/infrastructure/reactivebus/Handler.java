package com.codeicator.infrastructure.reactivebus;
import java.util.function.Consumer;


public class Handler {
    public final Class<?> Type;
    public final String Topic;
    public final Consumer<Object> Processor;

    public Handler(Consumer<Object> processor, Class<?> type,String topic) {
        Processor = processor;
        Type = type;
        Topic= topic;
    }

}
