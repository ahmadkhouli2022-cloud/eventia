package com.codeicator.infrastructure.reactivebus;

import com.codeicator.infrastructure.integration.StreamBridge;
import com.codeicator.messages.Message;
import com.codeicator.messages.Command;
import com.codeicator.messages.Event;
import com.codeicator.messages.Reply;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.time.Duration;
import java.util.UUID;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


public class ReactiveInmemoryBus extends Bus<Message>{
    private static final Logger log = LoggerFactory.getLogger(ReactiveInmemoryBus.class);

    private ReactiveInmemoryBus(ApplicationContext context){
        this.context=context;
        stream=new StreamBridge<>(Message.class);
    }
    public static Bus<Message> create(ApplicationContext context){
        var bus= new ReactiveInmemoryBus(context);
        bus.init();
        return bus;
    }

    private final StreamBridge<Message> stream;


    private void init(){
        try {
            super.registerHandlers();
            this.map(stream.getStream()).subscribe();
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            log.error(e.toString());
        }
    }
    protected Mono<Void> map(Flux<Message> flux) {

        return flux
                .handle((msg, sink) -> {
                    if ("Reply".equals(msg.getCategory())) {
                        var consumer = rpcMap.get(UUID.fromString(((Reply<?>)msg).getCorrelationId()));
                        if (consumer != null) {
                            consumer.accept((Reply<?>) msg);
                        }
                    }else {
                        var handlers = handlersMap.get(msg.getType());
                        if (handlers != null) {
                            handlers.forEach(handler ->{
                                try {
                                    log.debug("Processing message of type {} with handler {}", msg.getType(), handler);
                                    handler.Processor().accept(msg);

                                } catch (Exception e) {
                                    log.error("Error logging: handler "+handler.getClass()+ " message processing", e);
                                }
                            });
                        }
                    }
                    sink.next(msg);
                })
                .onErrorContinue((exp, obj) -> log.error("Handler raised an exception", exp))
                .then();
    }
    @Override
    public void raiseEvent(Event event){
        this.publish(this.eventBusDestination,event);
    }
    @Override
    public void raiseEvent(String destination,Event event){
        this.publish(destination,event);
    }
    @Override
    public void sendCommand(Command command){
        this.publish(this.commandBusDestination,command);
    }
    @Override
    public void sendCommand(String destination,Command command){
        this.publish(destination,command);
    }
    @Override
    public Mono<Void> sendCommand(Mono<Command> command){
        return command.doOnNext(msg->this.publish(this.commandBusDestination,msg)).then();
    }
    @Override
    public Mono<Void> sendCommand(String destination,Mono<Command> command){
        return command.doOnNext(msg->this.publish(destination,msg)).then();
    }

    @Override
    public Mono<Reply<?>> sendRPCCommand(Command command) {
        return rpc(rpcCommandBusDestination,command,Duration.ofSeconds(5));
    }

    @Override
    public Mono<Reply<?>> sendRPCCommand(Command command, Duration timeToResponse) {
        return rpc(rpcCommandBusDestination,command,timeToResponse);
    }

    @Override
    public Mono<Void> raiseEvent(Mono<Event> event) {
        return event.doOnNext(msg->this.publish(this.eventBusDestination,msg)).then();
    }
    @Override
    public Mono<Void> raiseEvent(String destination, Mono<Event> event) {
        return event.doOnNext(msg->this.publish(destination,msg)).then();
    }
    @Override
    public void publish(String destination, Message message) {
        stream.publish(message);
    }


    @Override
    public Mono<Reply<?>> sendRPCCommand(String destination, Command command) {

            return rpc(destination,command,Duration.ofSeconds(5));


    }
    @Override
    public Mono<Reply<?>> sendRPCCommand(String destination,Command command, Duration timeToResponse) {

            return rpc(destination,command,timeToResponse);

    }
    private Mono<Reply<?>> rpc(String destination,Command command, Duration timeToResponse){
        return Mono.<Reply<?>>create(sink->{
                    rpcMap.put(command.getId(), sink::success);
                    this.publish(destination,command);
                })
                .timeout(timeToResponse)
                .doFinally(s->rpcMap.remove(command.getId()));
    }

    @Override
    public void reply(String destination, Reply<?> reply) {
        this.publish(destination, reply);
    }

    @Override
    public Mono<Void> reply(String destination, Mono<Reply<?>> reply) {
        return reply.doOnNext(msg->this.publish(destination,msg)).then();

    }

}
