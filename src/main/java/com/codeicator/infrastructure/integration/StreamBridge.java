package com.codeicator.infrastructure.integration;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;

public class StreamBridge<T> {

    private static final Logger logger = LoggerFactory.getLogger(StreamBridge.class);
    private static final int BUFFER_SIZE = 1024;
    private final Flux<T> flux;
    private volatile Consumer<T> messageEmitter;
    private final AtomicInteger subscriberCount = new AtomicInteger(0);

    public StreamBridge(Class<T> type){
        flux = Flux.<T>create(fluxSink -> messageEmitter = fluxSink::next)
                .cast(type)
                .share()
                .publishOn(Schedulers.boundedElastic())
                .onBackpressureBuffer(
                        BUFFER_SIZE,
                        dropped -> logger.warn("Dropping message due to backpressure: {}", dropped),
                        reactor.core.publisher.BufferOverflowStrategy.DROP_OLDEST)
                .onErrorContinue((throwable, o) ->
                        logger.error("Error while processing message {}",
                                o == null ? "<null>" : o.getClass().getName(), throwable))
                .doOnSubscribe(subscription -> subscriberCount.incrementAndGet())
                .doOnCancel(subscriberCount::decrementAndGet);

    }

    public Flux<T> getStream(){
        return flux;
    }

    public void publish(T message){
        if (subscriberCount.get() == 0) {
            return;
        }
        Consumer<T> emitter = messageEmitter;
        if (emitter == null) {
            return;
        }
        emitter.accept(message);
    }
}
