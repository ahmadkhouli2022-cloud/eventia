package com.codeicator.infrastructure.reactivebus;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.codeicator.messages.Message;
import com.codeicator.messages.Command;
import com.codeicator.messages.Event;
import com.codeicator.messages.Reply;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.support.MessageBuilder;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Configuration
@Slf4j
public class ReactiveBus extends Bus<org.springframework.messaging.Message<String>> {

    @Autowired
    private StreamBridge stream;

    @Autowired
    private ObjectMapper objectMapper;

    @PostConstruct
    public void init(){

        try {
            this.registerHandlers();
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException e) {
            log.error(e.toString());
        }
    }

    public Function<Flux<org.springframework.messaging.Message<String>>,Mono<Void>> mapper(){
        return mapper("#");
    }

    public Function<Flux<org.springframework.messaging.Message<String>>,Mono<Void>> mapper(String topic){
        return flux->this.map(flux, topic);
    }
    private Mono<Void> map(Flux<org.springframework.messaging.Message<String>> flux, String topic) {

        return flux.filter(msg -> msg.getHeaders().get("type") != null && msg.getHeaders().get("category") != null)
                .handle((msg, sink) -> {
                    String category = String.valueOf(msg.getHeaders().get("category"));
                    String messageType = String.valueOf(msg.getHeaders().get("type"));

                    try {
                        if ("Reply".equals(category)) {
                            handleReplyMessage(msg, messageType);
                        } else {
                            handleDefaultMessage(msg, topic, messageType,category);
                        }
                    } catch (JsonProcessingException | ClassNotFoundException e) {
                        log.error("Error processing message", e);
                    }

                    sink.next(msg);
                })
                .onErrorContinue((exp, obj) -> log.error("Handler raised an exception", exp))
                .then();
    }

    private void handleReplyMessage(org.springframework.messaging.Message<String> msg, String messageType)
            throws JsonProcessingException, ClassNotFoundException {
        Reply reply = (Reply) objectMapper.readValue(msg.getPayload(), Class.forName(messageType));
        var consumer = super.rpcMap.get(UUID.fromString(reply.getCorrelationId()));
        if (consumer != null) {
            consumer.accept(reply);
        }
    }

    private void handleDefaultMessage(org.springframework.messaging.Message<String> msg, String topic, String messageType,String messageCategory) {
        processHandlers(msg, topic, handlersMap.get(messageType),messageType);

        if (messageCategory.equals("Command") || messageType.equals(String.class.getName()))
            return;
        List<Handler> jsonEventHandlers = handlersMap.get(String.class.getName());
        processHandlers(msg, topic, jsonEventHandlers,messageType);
    }

    private void processHandlers(org.springframework.messaging.Message<String> msg, String topic, List<Handler> handlers, String messageType) {
        if (handlers != null) {
            handlers.stream()
                    .filter(handler -> "#".equals(handler.Topic) || handler.Topic.equals(topic))
                    .forEach(handler -> {
                        if (handler.Type.equals(String.class)) {
                            log.debug("Handle json event {}" ,msg.getPayload());
                            processJsonEvent(handler, msg.getPayload());
                        }
                        else {
                            log.debug("Handle event {}" ,msg.getPayload());
                            processMessage(handler, msg.getPayload());
                        }

                    });
        }
    }

    private void processMessage(Handler handler,String msg){
        try {
            var objectMsg = objectMapper.readValue(msg, handler.Type);
            handler.Processor.accept(handler.Type.cast(objectMsg));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    private void processJsonEvent(Handler handler,String msg){
        try {
            var json = objectMapper.readTree(msg);
            var data="";
            if (json.get("data")==null)
                data=msg;
            else
                data=json.get("data").toString();

            Instant raisedAtInstant=null;
            var raisedAtJson=json.get("raisedAt").asText();
            if (raisedAtJson.matches("\\d+")) // all digits → treat as epoch
                raisedAtInstant = Instant.ofEpochMilli(Long.parseLong(raisedAtJson));
              else
                raisedAtInstant = Instant.parse(raisedAtJson);

            handler.Processor.accept(handler.Type.cast(data));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void publish(String destination,Message message){
        String type=message.getClass().getName();
        var toSend=MessageBuilder.withPayload(message)
                .setHeader("type", type)
                .setHeader("category", message.getCategory())
                .build();
        stream.send(destination, toSend);
    }

    public void raiseEvent(Event event){
        this.publish(this.eventBusDestination,event);
    }

    public void raiseEvent(String destination,Event event){
        this.publish(destination,event);
    }

    public void sendCommand(Command command){
        this.publish(this.commandBusDestination,command);
    }

    public void sendCommand(String destination,Command command){
        this.publish(destination,command);
    }

    public Mono<Void> sendCommand(Mono<Command> command){
        return command.doOnNext(msg->this.publish(this.commandBusDestination,msg)).then();
    }

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
