package com.mleibman.common.message.location.alert.kafka;

import com.mleibman.common.model.*;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

@Slf4j
@Configuration
public class LocationAlertKafkaStreamsConf {

    @Bean
    public KafkaStreamsConfiguration getKafkaStreamsConfiguration() {
        return new KafkaStreamsConfiguration(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "location_alerting_app_id_v1",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimeExtractor.class.getName()));
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

        personLocationDataStream
                .selectKey((personId, personLocationData) -> personLocationData.getLocationId())
                .peek((key, message) -> log.info("selectKey {}", message))
                .join(suspiciousLocationTable, (personLocationData, location)
                                -> new ExtendedPersonLocationData(personLocationData.getPersonId(), location, personLocationData.getTimestamp()),
                        Joined.with(Serdes.String(), new JsonSerde<>(PersonLocationData.class), new JsonSerde<>(Location.class)))
                .peek((key, message) -> log.info("join {}", message))
                .selectKey((personId, extendedPersonLocationData) -> extendedPersonLocationData.getPersonId())
                .peek((key, message) -> log.info("selectKey 2 {}", message))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(ExtendedPersonLocationData.class)))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(60)).grace(Duration.ZERO))
                .aggregate(this::init, this::agg,
                        Materialized.with(Serdes.String(), new JsonSerde<>(AggregatedPersonLocationData.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((key, message) -> log.info("after suppress {}", message))
                .filter((key, aggregatedPersonLocationData) -> aggregatedPersonLocationData.getExtendedPersonLocationDataList().size() > 1)
                .mapValues((key, aggregatedPersonLocationData) ->
                        new SuspiciousPersonLocationAlert(aggregatedPersonLocationData.getExtendedPersonLocationDataList().get(0).getPersonId(),
                                aggregatedPersonLocationData.getExtendedPersonLocationDataList()))
                .peek((key, message) -> log.info("Sending  suspicious person alert {}", message))
                .to("ALERT-PERSON-LOCATION-DATA",
                        Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), new JsonSerde<>(SuspiciousPersonLocationAlert.class)));
        return personLocationDataStream;
    }

    private AggregatedPersonLocationData init() {
        return new AggregatedPersonLocationData(Collections.emptyList());
    }

    private AggregatedPersonLocationData agg(String key, ExtendedPersonLocationData extendedPersonLocationData,
                                             AggregatedPersonLocationData aggregatedPersonLocationData) {
        ArrayList<ExtendedPersonLocationData> aggregatedPersonLocationDataList = new ArrayList<>(aggregatedPersonLocationData.getExtendedPersonLocationDataList());
        aggregatedPersonLocationDataList.add(extendedPersonLocationData);
        return new AggregatedPersonLocationData(aggregatedPersonLocationDataList);
    }
}
