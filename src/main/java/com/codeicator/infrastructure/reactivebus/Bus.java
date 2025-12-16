package com.codeicator.infrastructure.reactivebus;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.time.Duration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.codeicator.infrastructure.reactivebus.annotations.Handle;
import com.codeicator.messages.Message;
import com.codeicator.messages.Command;
import com.codeicator.messages.Event;
import com.codeicator.messages.Reply;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import reactor.core.publisher.Mono;


public abstract class Bus<T>{


    protected ApplicationContext context;
    @Value("${bus.command.destination:#{null}}")
    protected String commandBusDestination;
    @Value("${bus.rpc.command.destination:#{null}}")
    protected String rpcCommandBusDestination;
    @Value("${bus.event.destination:#{null}}")
    protected String eventBusDestination;
    protected final ConcurrentHashMap<String,List<Handler>> handlersMap=new ConcurrentHashMap<>();
    protected final ConcurrentHashMap<UUID,Consumer<Reply<?>>> rpcMap=new ConcurrentHashMap<>();
    protected ObjectMapper objectMapper;


    protected void registerHandlers() throws ClassNotFoundException, NoSuchMethodException {
        var handlers = context.getBeansWithAnnotation(Handle.class);

        for (var entry : handlers.entrySet()) {
            Object handlerInstance = entry.getValue();
            Class<?> handlerClass = handlerInstance.getClass();

            String realClassName = handlerClass.getName().split("\\$")[0];
            Class<?> realClass = getClass().getClassLoader().loadClass(realClassName);

            Method method = realClass.getMethod(entry.getKey());
            Handle annotation = method.getAnnotation(Handle.class);

            @SuppressWarnings("unchecked")
            Consumer<Object> func = (Consumer<Object>) handlerInstance;

            var messageHandler = new Handler(func, annotation.messageType(), annotation.topic());

            handlersMap.computeIfAbsent(annotation.messageType().getName(), key -> new ArrayList<>())
                    .add(messageHandler);
        }
    }




    public  abstract void publish(String destination,Message message);
    public  abstract void reply(String destination,Reply<?> reply);
    public  abstract Mono<Void> reply(String destination,Mono<Reply<?>> reply);
    public  abstract void raiseEvent(Event event);
    public  abstract void raiseEvent(String destination,Event event);
    public  abstract void sendCommand(Command command);
    public  abstract void sendCommand(String destination,Command command);
    public  abstract Mono<Void> sendCommand(Mono<Command> command);
    public  abstract Mono<Void> sendCommand(String destination,Mono<Command> command);
    public  abstract Mono<Reply<?>> sendRPCCommand(Command command);
    public  abstract Mono<Reply<?>> sendRPCCommand(Command command,Duration timeToResponse);
    public  abstract Mono<Reply<?>> sendRPCCommand(String destination,Command command);
    public  abstract Mono<Reply<?>> sendRPCCommand(String destination,Command command,Duration timeToResponse);

    public  abstract Mono<Void> raiseEvent(Mono<Event> event);
    public  abstract Mono<Void> raiseEvent(String destination,Mono<Event> event);


}
