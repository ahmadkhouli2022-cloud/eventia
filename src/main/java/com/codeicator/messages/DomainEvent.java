package com.codeicator.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.Date;

@Getter
@SuperBuilder(toBuilder = true)
public  abstract class DomainEvent extends Event{
    @JsonProperty("correlationId")
    private final String correlationId;
    @JsonProperty("orderId")
    private final int orderId;

}
