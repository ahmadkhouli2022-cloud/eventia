package com.codeicator.domain;

import com.codeicator.messages.Event;

public interface EventPublisher {
    void publish(Event event);

}
