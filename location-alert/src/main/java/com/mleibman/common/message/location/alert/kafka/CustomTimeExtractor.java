package com.mleibman.common.message.location.alert.kafka;

import com.mleibman.common.model.location.ExtendedPersonLocationData;
import com.mleibman.common.model.location.PersonLocationData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class CustomTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        final Object value = record.value();
        if (value instanceof PersonLocationData) {
            return ((PersonLocationData) value).getTimestamp();
        } else if (value instanceof ExtendedPersonLocationData) {
            return ((ExtendedPersonLocationData) value).getTimestamp();
        } else {
            return System.currentTimeMillis();
        }
    }
}