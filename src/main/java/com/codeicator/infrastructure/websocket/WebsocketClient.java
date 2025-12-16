package com.codeicator.infrastructure.websocket;

import java.util.concurrent.ConcurrentHashMap;

import lombok.NonNull;
import org.reactivestreams.Subscription;
import org.springframework.web.reactive.socket.WebSocketSession;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@RequiredArgsConstructor
public class WebsocketClient {

    @NonNull
    private WebSocketSession session;
    @Setter
    private String userId="Anaynomos";
    private ConcurrentHashMap<String, Subscription> subscribtions=new ConcurrentHashMap<>();

}
