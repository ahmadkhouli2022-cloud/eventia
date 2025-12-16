package com.codeicator.messages;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder(toBuilder = true)
public class Reply<T> extends Message {
    @JsonProperty("repliedAt")
    @Builder.Default
    private Date repliedAt=new Date();
    @JsonProperty("correlationId")
    private String correlationId;
    @JsonProperty("principalId")
    private String principalId;

    @JsonProperty("destinationId")
    private String destinationId;

    @Builder.Default
    protected String category="Reply";
    @JsonProperty
    private String subscriptionId;
    @JsonProperty
    private T data;
}
