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
    private final List<PersonLocationData> personLocationDataList;

    @JsonCreator
    public AggregatedPersonLocationData(@JsonProperty("personLocationDataList") List<PersonLocationData> personLocationDataList) {
        this.personLocationDataList = personLocationDataList;
    }
}