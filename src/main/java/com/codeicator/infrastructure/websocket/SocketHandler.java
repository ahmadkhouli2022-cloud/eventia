package com.codeicator.infrastructure.websocket;

import java.util.function.Supplier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.codeicator.messages.Command;
import com.codeicator.messages.websocket.Unsubscribe;
import com.codeicator.messages.websocket.Completed;
import com.codeicator.messages.websocket.Subscribed;
import com.codeicator.messages.websocket.Unsubscribed;
import lombok.NonNull;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Slf4j
public class SocketHandler implements WebSocketHandler{

    private final ObjectMapper mapper;
    private final WebSocketRouter router;

    private SocketHandler(ObjectMapper mapper, WebSocketRouter router) {
        this.mapper = mapper;
        this.router = router;
    }

    public static SocketHandler create(ObjectMapper mapper, WebSocketRouter router){
        return new SocketHandler(mapper,router);
    }

    @Override
    @NonNull
    public Mono<Void> handle( @NonNull WebSocketSession session) {
//        if (session ==null) return Mono.error(new Throwable("Session is null"));
        WebsocketClient client=new WebsocketClient(session);
        return session.receive()
            .map(WebSocketMessage::getPayloadAsText)
            .map(this::extractType)
            .map(this::mapMessageToCommand)
            .flatMap(c->{
                if (c instanceof Unsubscribe)
                    return Mono.just(c);

                if (client.getSubscribtions().containsKey(c.getId().toString())){
                    return session.send(Mono.fromSupplier(()-> c.reply("\"Already Subscribed\"", "exception"))
                    .map(o->this.constructMessage(o,session)))
                    .then(Mono.just(c));
                }
                client.setUserId(c.getPrincipalId());
                var messages=Flux.from(router.routeCommand(c,client));

                var subscribed=Subscribed.builder()
                    .correlationId(c.getId().toString())
                    .destinationId(c.getPrincipalId())
                    .subscriptionId(c.getUri()).build();

                var completed=Completed.builder()
                    .correlationId(c.getId().toString())
                    .destinationId(c.getPrincipalId())
                    .subscriptionId(c.getUri()).build();

                return session.send(messages
                        .map(msg-> constructMessage(msg,session))
                        .startWith(constructMessage(subscribed, session))
                        .concatWithValues(constructMessage(completed, session)))

                    .doOnSubscribe(subscription->client.getSubscribtions().putIfAbsent(c.getId().toString(), subscription))
                    .doFinally(s->{
                        if (router.onClose!=null)
                            router.onClose.accept(client);
                        client.getSubscribtions().remove(c.getUri());
                    })
                    .onErrorContinue((exp,obj)->log.error("An exception occurred!", exp));

            })
            .filter(p->p.getClass().isAssignableFrom(Unsubscribe.class))
            .cast(Unsubscribe.class)
            .doOnNext(c->{
                var s= client.getSubscribtions().remove(c.getSubscriptionId());
                var completed=Mono.just(Unsubscribed.builder()
                                        .destinationId(c.getPrincipalId())
                                        .subscriptionId(c.getSubscriptionId())
                                        .correlationId(c.getId().toString())
                                        .build())
                                .map(m-> constructMessage(m, session));
                session.send(completed).subscribe();
                if (s!=null)
                    s.cancel();
            })
            .onErrorContinue((exp,obj)->log.error("An exception occurred!", exp))
            .then();
    }


    public Supplier<Flux<WebSocketMessage>> publisher(){
         return ()->Flux.create(sink->{

        });
    }
    public Tuple2<Command,String> extractType(String msg){
        try {
            var obj=mapper.readValue(msg, Command.class);
            return Tuples.of(obj, msg);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
    public Command mapMessageToCommand(Tuple2<Command,String> msg){
        try {
            var c=Class.forName(msg.getT1().getType());
                return (Command)mapper.readValue(msg.getT2(),c);

        } catch (ClassNotFoundException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
    public WebSocketMessage constructMessage(Object event, WebSocketSession session) {
        try {
            return session.textMessage(mapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
