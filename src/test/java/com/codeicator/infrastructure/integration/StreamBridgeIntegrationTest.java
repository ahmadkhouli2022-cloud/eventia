package com.codeicator.infrastructure.integration;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamBridgeIntegrationTest {

    @Test
    void publishBeforeSubscribeDoesNotFailAndDoesNotDeliver() {
        StreamBridge<String> bridge = new StreamBridge<>(String.class);

        bridge.publish("early");

        String received = bridge.getStream()
                .next()
                .timeout(Duration.ofMillis(200))
                .onErrorReturn("none")
                .block();

        assertEquals("none", received);
    }

    @Test
    void publishAfterSubscribeDeliversMessage() {
        StreamBridge<String> bridge = new StreamBridge<>(String.class);

        String received = bridge.getStream()
                .next()
                .doOnSubscribe(s ->
                        Schedulers.boundedElastic().schedule(() -> bridge.publish("hello"), 50, TimeUnit.MILLISECONDS))
                .block(Duration.ofSeconds(2));

        assertEquals("hello", received);
    }

    @Test
    void multipleSubscribersReceiveNewMessages() {
        StreamBridge<String> bridge = new StreamBridge<>(String.class);

        CopyOnWriteArrayList<String> first = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> second = new CopyOnWriteArrayList<>();

        Flux<String> stream = bridge.getStream();
        stream.take(2).subscribe(first::add);
        stream.take(2).subscribe(second::add);

        bridge.publish("m1");
        bridge.publish("m2");

        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }

        assertEquals(2, first.size());
        assertEquals(2, second.size());
        assertTrue(first.contains("m1") && first.contains("m2"));
        assertTrue(second.contains("m1") && second.contains("m2"));
    }
}
