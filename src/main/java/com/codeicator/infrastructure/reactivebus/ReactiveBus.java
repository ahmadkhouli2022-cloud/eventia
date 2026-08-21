package com.codeicator.infrastructure.reactivebus;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.codeicator.messages.Message;
import com.codeicator.messages.Command;
import com.codeicator.messages.Event;
import com.codeicator.messages.Reply;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveBus extends Bus<org.springframework.messaging.Message<String>> {

    private static final Logger log = LoggerFactory.getLogger(ReactiveBus.class);

    private ReactiveBus(ApplicationContext context,StreamBridge streamBridge){
        this.context=context;
        this.stream=streamBridge;
        objectMapper=new ObjectMapper();
    }

    public static Bus<org.springframework.messaging.Message<String>> create(ApplicationContext context,StreamBridge streamBridge){
        var bus=new ReactiveBus(context,streamBridge);
        bus.init();
        return bus;
    }

    private final StreamBridge stream;

    private final ObjectMapper objectMapper;

    private void init(){

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
                            handleReplyMessage(msg.getPayload(), messageType);
                        } else {
                            handleDefaultMessage(msg.getPayload(), topic, messageType,category);
                        }
                    } catch (JsonProcessingException | ClassNotFoundException e) {
                        log.error("Error processing message", e);
                    }

                    sink.next(msg);
                })
                .onErrorContinue((exp, obj) -> log.error("Handler raised an exception", exp))
                .then();
    }

    private void handleReplyMessage(String msg, String messageType)
            throws JsonProcessingException, ClassNotFoundException {
        Reply<?> reply = (Reply<?>) objectMapper.readValue(msg, Class.forName(messageType));
        var consumer = super.rpcMap.get(UUID.fromString(reply.getCorrelationId()));
        if (consumer != null) {
            consumer.accept(reply);
        }
    }

    private void handleDefaultMessage(String msg, String topic, String messageType,String messageCategory) {
        processHandlers(msg, topic, handlersMap.get(messageType));

        if (messageCategory.equals("Command") || messageType.equals(String.class.getName()))
            return;
        List<Handler> jsonEventHandlers = handlersMap.get(String.class.getName());
        processHandlers(msg, topic, jsonEventHandlers);
    }

    private void processHandlers( String msg, String topic, List<Handler> handlers) {
        if (handlers != null) {
            handlers.stream()
                    .filter(handler -> "#".equals(handler.Topic()) || handler.Topic().equals(topic))
                    .forEach(handler -> {
                        if (handler.Type().equals(String.class)) {
                            log.debug("Handle json event {}" ,msg);
                            processJsonEvent(handler, msg);
                        }
                        else {
                            log.debug("Handle event {}" ,msg);
                            processMessage(handler, msg);
                        }

                    });
        }
    }

    private void processMessage(Handler handler,String msg){
        try {
            var objectMsg = objectMapper.readValue(msg, handler.Type());
            handler.Processor().accept(handler.Type().cast(objectMsg));
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

            /*
              Instant raisedAtInstant=null;
                        var raisedAtJson=json.get("raisedAt").asText();
                        if (raisedAtJson.matches("\\d+")) // all digits → treat as epoch
                            raisedAtInstant = Instant.ofEpochMilli(Long.parseLong(raisedAtJson));
                          else
                            raisedAtInstant = Instant.parse(raisedAtJson);
                        var message= JsonEvent.builder().id(UUID.fromString(json.get("id").asText()))
                            .category(json.get("category").asText())
                            .type(json.get("type").asText())
                            .streamId(json.get("streamId").asText())
                            .correlationId(json.get("correlationId").asText())
                            .orderId(json.get("orderId").asInt(0))
                            .data(data)
                            .streamType(json.get("streamType").asText())
                            .raisedAt(Date.from(raisedAtInstant))
                            .build();
            */
            handler.Processor().accept(handler.Type().cast(data));

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

    /**
     * Publishes the raw JSON payload as-is with the {@code type} and {@code category} headers the
     * consumer-side mapper dispatches on — no local deserialization, so outbox rows owned by other
     * services can be relayed without their event classes.
     */
    @Override
    public void raiseRawEvent(String topic, String eventType, String payload){
        var toSend=MessageBuilder.withPayload(payload)
                .setHeader("type", eventType)
                .setHeader("category", "Event")
                .setHeader("contentType", "application/json")
                .build();
        stream.send(topic, toSend);
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
    public Mono<Reply<?>> sendRPCCommand(String destination,Command command) {
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
