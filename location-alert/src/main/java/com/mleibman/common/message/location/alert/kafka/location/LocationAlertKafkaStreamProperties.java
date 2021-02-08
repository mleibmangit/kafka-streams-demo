package com.mleibman.common.message.location.alert.kafka.location;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class LocationAlertKafkaStreamProperties {
    private final long windowSizeSeconds;
    private final int minimumSizeOfSuspiciousVisits;
}
