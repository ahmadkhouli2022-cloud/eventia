package com.codeicator.infrastructure.websocket;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;

import com.codeicator.messages.Message;
import com.codeicator.messages.Command;

import reactor.core.publisher.Mono;

public class WebSocketRouter {
    private Map<String,BiFunction< Command,WebsocketClient,Publisher<? extends Message>>> handlers=new HashMap<>();
    public Consumer<WebsocketClient> onClose;

    public Publisher<? extends Message> routeCommand(Command command,WebsocketClient client){
        var func=handlers.get(command.getUri());
        if (func==null)
            return Mono.just(command.reply("\"END POINT NOT FOUND\"", "exception"));
        return func.apply(command,client);

    }
    public void addCommandHandler(String uri,BiFunction<Command,WebsocketClient,Publisher<? extends Message>> handler){
        handlers.put(uri, handler);
    }


}
