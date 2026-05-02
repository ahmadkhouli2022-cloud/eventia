package com.codeicator.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
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
    @Builder.Default
    @JsonProperty("version")
    protected long version=1;
    @Builder.Default
    protected final String category="Event";
    @JsonProperty("schemaVersion")
    @Builder.Default
    private final int schemaVersion = 1;

}
