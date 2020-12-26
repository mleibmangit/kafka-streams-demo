package com.mleibman.common.message.location.alert.kafka;

import com.mleibman.common.model.PersonLocationData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class CustomTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        final Object value = record.value();
        if(value instanceof PersonLocationData) {
            return ((PersonLocationData) value).getTimestamp();
        } else {
            return System.currentTimeMillis();
        }
    }
}