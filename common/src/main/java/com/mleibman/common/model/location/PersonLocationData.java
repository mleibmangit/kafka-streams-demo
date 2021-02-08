package com.mleibman.common.model.location;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mleibman.common.model.KafkaIncomingData;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@Getter
public class PersonLocationData implements KafkaIncomingData {
    private final String personId;
    private final String locationId;
    private final long timestamp;

    @JsonCreator
    public PersonLocationData(@JsonProperty("personId") String personId,
                              @JsonProperty("locationId") String locationId,
                              @JsonProperty("timestamp") long timestamp) {
        this.personId = personId;
        this.locationId = locationId;
        this.timestamp = timestamp;
    }
}