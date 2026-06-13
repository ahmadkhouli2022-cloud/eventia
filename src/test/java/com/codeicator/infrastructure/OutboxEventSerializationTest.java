package com.codeicator.infrastructure;

import com.codeicator.messages.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class OutboxEventSerializationTest {

    @Jacksonized
    @SuperBuilder(toBuilder = true)
    private static class TestEvent extends Event {
    }

    @Test
    void outboxEventSerializationRoundtrip() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        UUID id = UUID.randomUUID();
        TestEvent event = TestEvent.builder()
            .streamId(id.toString())
            .streamType("test")
            .correlationId("corr")
            .orderId(1)
            .build();

        OutboxEvent outbox = OutboxEvent.from(event, mapper);
        assertEquals(event.getSchemaVersion(), outbox.getSchemaVersion());
        assertEquals(event.getId(), outbox.getId());
        assertNotNull(outbox.getEventPayload());

        Event deserialized = outbox.getEvent(mapper);
        assertNotNull(deserialized);
        assertEquals(event.getType(), deserialized.getType());
        assertEquals(event.getStreamId(), deserialized.getStreamId());
    }
}

