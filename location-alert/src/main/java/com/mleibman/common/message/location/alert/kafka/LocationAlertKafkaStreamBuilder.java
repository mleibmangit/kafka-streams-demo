package com.mleibman.common.message.location.alert.kafka;

import com.mleibman.common.model.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;

@Slf4j
@Service
public class LocationAlertKafkaStreamBuilder {

    private final StreamsBuilder streamBuilder;

    @Autowired
    public LocationAlertKafkaStreamBuilder(StreamsBuilder streamBuilder) {
        this.streamBuilder = streamBuilder;
    }

    public KStream<String, PersonLocationData> buildLocationAlertKafkaStream_GlobalKTable(LocationAlertKafkaStreamProperties locationAlertKafkaStreamProperties) {

        KStream<String, PersonLocationData> personLocationDataStream = streamBuilder.stream("PERSON-LOCATION-DATA",
                Consumed.with(Serdes.String(), new JsonSerde<>(PersonLocationData.class)));

        GlobalKTable<String, Location> suspiciousLocationTable = streamBuilder.globalTable("SUSPICIOUS-LOCATION-DATA",
                Consumed.with(Serdes.String(), new JsonSerde<>(Location.class)));

        personLocationDataStream
                /*.selectKey((personId, personLocationData) -> personLocationData.getLocationId())
                .peek((key, message) -> log.info("selectKey {}", message))*/
                .join(suspiciousLocationTable,
                        (personId, personLocationData) -> personLocationData.getLocationId(),
                        (personLocationData, location)
                                -> new ExtendedPersonLocationData(personLocationData.getPersonId(), location, personLocationData.getTimestamp()))
                .peek((key, message) -> log.info("join {}", message))
                .selectKey((personId, extendedPersonLocationData) -> extendedPersonLocationData.getPersonId())
                .peek((key, message) -> log.info("selectKey 2 {}", message))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(ExtendedPersonLocationData.class)))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(locationAlertKafkaStreamProperties.getWindowSizeSeconds())).grace(Duration.ZERO))
                .aggregate(this::init, this::agg,
                        Materialized.with(Serdes.String(), new JsonSerde<>(AggregatedPersonLocationData.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((key, message) -> log.info("after suppress {}", message))
                .filter((key, aggregatedPersonLocationData)
                        -> aggregatedPersonLocationData.getExtendedPersonLocationDataList().size() > locationAlertKafkaStreamProperties.getMinimumSizeOfSuspiciousVisits())
                .mapValues((key, aggregatedPersonLocationData) ->
                        new SuspiciousPersonLocationAlert(aggregatedPersonLocationData.getExtendedPersonLocationDataList().get(0).getPersonId(),
                                aggregatedPersonLocationData.getExtendedPersonLocationDataList()))
                .peek((key, message) -> log.info("Sending  suspicious person alert {}", message))
                .to("ALERT-PERSON-LOCATION-DATA",
                        Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), new JsonSerde<>(SuspiciousPersonLocationAlert.class)));
        return personLocationDataStream;
    }


    public KStream<String, PersonLocationData> buildLocationAlertKafkaStream(LocationAlertKafkaStreamProperties locationAlertKafkaStreamProperties) {

        KStream<String, PersonLocationData> personLocationDataStream = streamBuilder.stream("PERSON-LOCATION-DATA",
                Consumed.with(Serdes.String(), new JsonSerde<>(PersonLocationData.class)));

        KTable<String, Location> suspiciousLocationTable = streamBuilder.table("SUSPICIOUS-LOCATION-DATA",
                Consumed.with(Serdes.String(), new JsonSerde<>(Location.class)));

        personLocationDataStream
                /* .selectKey((personId, personLocationData) -> personLocationData.getLocationId())
                 .peek((key, message) -> log.info("selectKey {}", message))*/
                .join(suspiciousLocationTable, (personLocationData, location)
                                -> new ExtendedPersonLocationData(personLocationData.getPersonId(), location, personLocationData.getTimestamp()),
                        Joined.with(Serdes.String(), new JsonSerde<>(PersonLocationData.class), new JsonSerde<>(Location.class)))
                .peek((key, message) -> log.info("join {}", message))
                /* .selectKey((personId, extendedPersonLocationData) -> extendedPersonLocationData.getPersonId())
                 .peek((key, message) -> log.info("selectKey 2 {}", message))*/
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(ExtendedPersonLocationData.class)))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(locationAlertKafkaStreamProperties.getWindowSizeSeconds())).grace(Duration.ZERO))
                .aggregate(this::init, this::agg,
                        Materialized.with(Serdes.String(), new JsonSerde<>(AggregatedPersonLocationData.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((key, message) -> log.info("after suppress {}", message))
                .filter((key, aggregatedPersonLocationData)
                        -> aggregatedPersonLocationData.getExtendedPersonLocationDataList().size() > locationAlertKafkaStreamProperties.getMinimumSizeOfSuspiciousVisits())
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
