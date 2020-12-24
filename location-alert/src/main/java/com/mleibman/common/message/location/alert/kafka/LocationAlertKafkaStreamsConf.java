package com.mleibman.common.message.location.alert.kafka;

import com.mleibman.common.model.AggregatedPersonLocationData;
import com.mleibman.common.model.Location;
import com.mleibman.common.model.PersonLocationData;
import com.mleibman.common.model.SuspiciousPersonLocationAlert;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.util.Collections;
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

        KStream<String, PersonLocationData> personLocationDataStream = streamBuilder.stream("PERSON-LOCATION-DATA",
                Consumed.with(Serdes.String(), new JsonSerde<>(PersonLocationData.class)));

        KTable<String, Location> suspiciousLocationTable = streamBuilder.table("SUSPICIOUS-LOCATION-DATA",
                Consumed.with(Serdes.String(), new JsonSerde<>(Location.class)));

       /* personLocationDataStream
                .mapValues(personLocationData -> new SuspiciousPersonLocationAlert(personLocationData.getPersonId(), List.of(personLocationData)))
                .peek((key, message) -> log.info("Sending {}", message))
                .to("ALERT-PERSON-LOCATION-DATA", Produced.with(Serdes.String(), new JsonSerde<>(SuspiciousPersonLocationAlert.class)));*/

        personLocationDataStream
                .selectKey((personId, personLocationData) -> personLocationData.getLocation().getLocationId())
                .join(suspiciousLocationTable, (personLocationData, location) -> personLocationData)
                .selectKey((personId, personLocationData) -> personLocationData.getPersonId())
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(60)).grace(Duration.ZERO))
                .aggregate(this::init, this::agg)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .mapValues((k, m) -> new SuspiciousPersonLocationAlert(m.getPersonLocationDataList().get(0).getPersonId(), m.getPersonLocationDataList()))
                .peek((key, message) -> log.info("Sending  suspicious alert {}", message))
                .to("ALERT-PERSON-LOCATION-DATA");

        return personLocationDataStream;
    }

    private AggregatedPersonLocationData init() {
        return new AggregatedPersonLocationData(Collections.emptyList());
    }

    private AggregatedPersonLocationData agg(String s, PersonLocationData object, AggregatedPersonLocationData aggregatedPersonLocationData) {
        return null;
    }
}
