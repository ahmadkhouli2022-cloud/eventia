package com.codeicator.infrastructure;

import com.codeicator.messages.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OutboxEventTest {

    @Jacksonized
    @SuperBuilder(toBuilder = true)
    private static class TestEvent extends Event {
    }

    @Test
    void fromAndGetEventRoundTrip() {
        ObjectMapper objectMapper = new ObjectMapper();
        TestEvent event = TestEvent.builder()
            .streamId("stream-2")
            .streamType("test")
            .correlationId("corr-2")
            .orderId(2)
            .build();

        OutboxEvent outboxEvent = OutboxEvent.from(event, objectMapper);
        Event restored = outboxEvent.getEvent(objectMapper);

        assertNotNull(outboxEvent.getId());
        assertEquals(event.getId(), outboxEvent.getId());
        assertEquals(event.getStreamId(), restored.getStreamId());
        assertEquals(event.getStreamType(), restored.getStreamType());
        assertEquals(event.getCorrelationId(), restored.getCorrelationId());
        assertEquals(event.getOrderId(), restored.getOrderId());
        assertEquals(event.getSchemaVersion(), outboxEvent.getSchemaVersion());
    }
}
