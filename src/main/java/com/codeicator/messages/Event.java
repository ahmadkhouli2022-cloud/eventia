package com.codeicator.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import java.util.Date;
@Getter
@SuperBuilder(toBuilder = true)
public abstract class Event extends Message{
    @JsonProperty("raisedAt")
    @Builder.Default
    private final Date raisedAt=new Date();
    @JsonProperty("streamId")
    private final String streamId;
    @JsonProperty("streamType")
    private final String streamType;
    @JsonProperty("correlationId")
    private final String correlationId;
    @JsonProperty("orderId")
    private final int orderId;
    @JsonProperty("version")
    @Builder.Default
    private long version = 0;
    @Builder.Default
    protected final String category="Event";

    public void setVersion(long version) {
        this.version = version;
    }
}
