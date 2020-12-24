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
public class SuspiciousPersonLocationAlert {
    private final String personId;
    private final List<PersonLocationData> personLocationDataList;

    @JsonCreator
    public SuspiciousPersonLocationAlert(@JsonProperty("personId") String personId,
                                         @JsonProperty("personLocationDataList") List<PersonLocationData> personLocationDataList) {
        this.personId = personId;
        this.personLocationDataList = personLocationDataList;
    }
}