package com.codeicator.messages;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.util.Date;
@Getter
@SuperBuilder(toBuilder = true)
public abstract class Command extends Message {
    @Builder.Default
    @JsonProperty("queuedAt")
    private Date queuedAt=new Date();
    @JsonProperty("uri")
    private String uri;
    @JsonProperty("principalId")
    private String principalId;
    @Builder.Default
    protected String category="Command";
    @JsonIgnore
    public <T >Reply<T> reply(T data, String exception){
        return (Reply<T>) Reply.builder()
                .correlationId(this.getId().toString())
                .destinationId(this.getPrincipalId())
                .subscriptionId(this.getUri())
                .type(data.getClass().getName())
                .data(data)
                .build();
    }


}
