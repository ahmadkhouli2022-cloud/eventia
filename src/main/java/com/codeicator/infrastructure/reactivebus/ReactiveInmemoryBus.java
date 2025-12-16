package com.codeicator.infrastructure.reactivebus;

import com.codeicator.infrastructure.integration.StreamBridge;
import com.codeicator.messages.Message;
import com.codeicator.messages.Command;
import com.codeicator.messages.Event;
import com.codeicator.messages.Reply;

import jakarta.annotation.PostConstruct;

import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Configuration
@Slf4j
public class ReactiveInmemoryBus extends Bus<Message>{
    private final StreamBridge<Message> stream=new StreamBridge<>(Message.class);


    @PostConstruct
    public void init(){
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
                    var consumer = rpcMap.get(msg.getType());
                    if (consumer != null) {
                        consumer.accept(msg);
                    }
                    var handlers=handlersMap.get(msg.getType());
                    if (handlers != null) {
                        handlers.forEach(handler -> handler.Processor.accept(msg));
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
    public Mono<Reply> sendRPCCommand(String destination,Mono<Command> command) {
        return command.flatMap(msg->{
            return rpc(destination,msg,Duration.ofSeconds(5));

        });
    }
    @Override
    public Mono<Reply> sendRPCCommand(String destination,Mono<Command> command, Duration timeToResponse) {
        return command.flatMap(msg->{
            return rpc(destination,msg,timeToResponse);

        });
    }
    private Mono<Reply> rpc(String destination,Command command, Duration timeToResponse){
        return Mono.create(sink->{
                    rpcMap.put(command.getId(), sink::success);
                    this.publish(destination,command);
                })
                .timeout(timeToResponse)
                .cast(Reply.class)
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
