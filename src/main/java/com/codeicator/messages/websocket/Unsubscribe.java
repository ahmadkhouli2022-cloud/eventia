package com.codeicator.messages.websocket;

import com.codeicator.messages.Command;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import lombok.extern.jackson.Jacksonized;

@Value
@EqualsAndHashCode(callSuper=false)
@SuperBuilder(toBuilder = true)
@Jacksonized
public class Unsubscribe extends Command {
    @JsonProperty
    String subscriptionId;
}
