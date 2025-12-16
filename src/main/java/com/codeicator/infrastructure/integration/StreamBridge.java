package com.codeicator.infrastructure.integration;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

public class StreamBridge<T> {

    private static final Logger logger = LoggerFactory.getLogger(StreamBridge.class);
    private final Flux<T> flux ;
    private Consumer<T> messageEmitter;
    private final AtomicInteger subscriberCount = new AtomicInteger(0);

    public StreamBridge(Class<T> type){
        flux= Flux.create(fluxSink ->messageEmitter=fluxSink::next)
                .cast(type)
                .share()
                .publishOn(Schedulers.boundedElastic())
                .onBackpressureBuffer()
                .onErrorContinue((throwable, o) ->
                        logger.error("Error  while processing    message {}", o.getClass().getName(), throwable))
                .doOnSubscribe(subscription -> subscriberCount.incrementAndGet())
                .doOnCancel(subscriberCount::decrementAndGet);

    }

    public Flux<T> getStream(){
        return flux;
    }

    public void publish(T message){
        if (subscriberCount.get()==0)
            return;
        messageEmitter.accept(message);
    }
}
