package com.mleibman.common.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@AllArgsConstructor
public class PersonLocationData implements KafkaIncomingData {
    private final String personId;
    private final Location location;
    private final long timestamp;
}