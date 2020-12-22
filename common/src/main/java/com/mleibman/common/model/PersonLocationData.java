package com.mleibman.common.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@AllArgsConstructor
public class PersonLocationData {
    private final long personId;
    private final Location location;
}