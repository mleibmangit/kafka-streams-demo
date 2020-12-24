package com.mleibman.common.message.location.alert.kafka;

import com.mleibman.common.model.PersonLocationData;
import com.mleibman.common.model.SuspiciousPersonLocationAlert;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
public class LocationAlertKafkaStreamsConf {

    @Bean
    public KafkaStreamsConfiguration getKafkaStreamsConfiguration() {
        return new KafkaStreamsConfiguration(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "location_alerting_app_id_v1",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));
    }

    @Bean
    public FactoryBean<StreamsBuilder> streamBuilder(KafkaStreamsConfiguration streamsConfig) {
        return new StreamsBuilderFactoryBean(streamsConfig);
    }

    @Bean
    public KStream<?, ?> kStream(StreamsBuilder streamBuilder) {
        //PERSON-LOCATION-DATA


        //KStream<Long, Location> locationDataStream = kStreamBuilder.stream("LOCATION-DATA");
        KStream<String, PersonLocationData> personLocationDataStream = streamBuilder.stream("PERSON-LOCATION-DATA",
                Consumed.with(Serdes.String(), new JsonSerde<>(PersonLocationData.class)));

        personLocationDataStream
                .mapValues(personLocationData -> new SuspiciousPersonLocationAlert(personLocationData.getPersonId(), List.of(personLocationData)))
                .peek((key, message) -> log.info("Sending {}", message))
                .to("ALERT-PERSON-LOCATION-DATA", Produced.with(Serdes.String(), new JsonSerde<>(SuspiciousPersonLocationAlert.class)));

        return personLocationDataStream;
    }
}
