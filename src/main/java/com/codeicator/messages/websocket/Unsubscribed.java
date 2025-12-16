package com.codeicator.messages.websocket;

import com.codeicator.messages.Reply;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder(toBuilder = true)
public class Unsubscribed extends Reply<String> {

    @JsonProperty("subscriptionId")
    private String subscriptionId;

}
