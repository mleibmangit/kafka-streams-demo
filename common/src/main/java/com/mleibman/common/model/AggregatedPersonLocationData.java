package com.mleibman.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@ToString
@EqualsAndHashCode
@Getter
public class AggregatedPersonLocationData {
    private final List<ExtendedPersonLocationData> extendedPersonLocationDataList;

    @JsonCreator
    public AggregatedPersonLocationData(@JsonProperty("extendedPersonLocationDataList") List<ExtendedPersonLocationData> extendedPersonLocationDataList) {
        this.extendedPersonLocationDataList = extendedPersonLocationDataList;
    }
}