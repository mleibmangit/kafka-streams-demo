package com.mleibman.common.message.generator.kafka;

import com.mleibman.common.model.KafkaIncomingData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
public class KafkaSender {

    private final KafkaTemplate<String, KafkaIncomingData> kafkaTemplate;

    @Autowired
    public KafkaSender(KafkaTemplate<String, KafkaIncomingData> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String topic, String key, KafkaIncomingData data) {

        try {
            SendResult<String, KafkaIncomingData> sendResult = kafkaTemplate.send(topic, key, data).get(10, TimeUnit.SECONDS);
            handleSuccess(topic, data, sendResult);
        } catch (ExecutionException e) {
            handleFailure(topic, data, e.getCause());
        } catch (TimeoutException | InterruptedException e) {
            handleFailure(topic, data, e);
        }
    }

    private void handleSuccess(String topic, KafkaIncomingData data, SendResult<String, KafkaIncomingData> sendResult) {
        log.info("Data {} was successfully sent to topic {}, record {}", topic, data, sendResult.getProducerRecord());

    }

    private void handleFailure(String topic, KafkaIncomingData data, Throwable cause) {
        log.error("Failed to send data {}  to topic {}, error {}", topic, data, cause.getMessage());
    }
}