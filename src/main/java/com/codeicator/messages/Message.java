package com.codeicator.messages;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.Serializable;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

@SuperBuilder(toBuilder = true)
@ToString
public abstract class Message implements Serializable{
    @Getter
    @Builder.Default
    @JsonProperty("id")
    private UUID id = UUID.randomUUID();

    @JsonProperty("type")
    private String type;

    @Getter
    @Builder.Default
    @JsonProperty("category")
    protected String category="Unspecified";


    public String getType(){
        return type==null? this.getClass().getName():this.type;
    }

}
