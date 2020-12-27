package com.mleibman.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@Getter
public class ExtendedPersonLocationData implements KafkaIncomingData {
    private final String personId;
    private final Location location;
    private final long timestamp;

    @JsonCreator
    public ExtendedPersonLocationData(@JsonProperty("personId") String personId,
                                      @JsonProperty("location") Location location,
                                      @JsonProperty("timestamp") long timestamp) {
        this.personId = personId;
        this.location = location;
        this.timestamp = timestamp;
    }
}