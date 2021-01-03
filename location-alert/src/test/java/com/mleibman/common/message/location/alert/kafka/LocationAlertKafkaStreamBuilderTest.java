package com.mleibman.common.message.location.alert.kafka;

import com.mleibman.common.model.Location;
import com.mleibman.common.model.PersonLocationData;
import com.mleibman.common.model.SuspiciousPersonLocationAlert;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocationAlertKafkaStreamBuilderTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, PersonLocationData> inputPersonLocationDataTopic;
    private TestInputTopic<String, Location> inputLocationDataTopic;
    private TestOutputTopic<Windowed<String>, SuspiciousPersonLocationAlert> outputTopic;

    @BeforeEach
    public void init() {

        StreamsBuilder builder = new StreamsBuilder();
        LocationAlertKafkaStreamBuilder locationAlertKafkaStreamBuilder = new LocationAlertKafkaStreamBuilder(builder);
        locationAlertKafkaStreamBuilder
                .buildLocationAlertKafkaStream(new LocationAlertKafkaStreamProperties(3, 1));

        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimeExtractor.class.getName());
        testDriver = new TopologyTestDriver(topology, props);

        inputPersonLocationDataTopic = testDriver.createInputTopic("PERSON-LOCATION-DATA",
                Serdes.String().serializer(), new JsonSerde<>(PersonLocationData.class).serializer());
        inputLocationDataTopic = testDriver.createInputTopic("SUSPICIOUS-LOCATION-DATA",
                Serdes.String().serializer(), new JsonSerde<>(Location.class).serializer());
        outputTopic = testDriver.createOutputTopic("ALERT-PERSON-LOCATION-DATA",
                WindowedSerdes.timeWindowedSerdeFrom(String.class).deserializer(), new JsonSerde<>(SuspiciousPersonLocationAlert.class).deserializer());
    }

    @Test
    public void testLocationAlertStreamApp() throws InterruptedException {

        Location location1 = new Location("1", 1, 0);
        Location location2 = new Location("2", 2, 0);
        Location location3 = new Location("3", 3, 0);
        Location location4 = new Location("4", 4, 0);

        Instant now = Instant.now();

        PersonLocationData personLocationData1 = new PersonLocationData("1", location1.getLocationId(), now.minusSeconds(15).toEpochMilli());
        PersonLocationData personLocationData2 = new PersonLocationData("1", location2.getLocationId(), now.minusSeconds(14).toEpochMilli());
        PersonLocationData personLocationData3 = new PersonLocationData("1", location3.getLocationId(), now.minusSeconds(13).toEpochMilli());
        PersonLocationData personLocationData4 = new PersonLocationData("1", location4.getLocationId(), now.minusSeconds(12).toEpochMilli());
       /* PersonLocationData personLocationData5 = new PersonLocationData("1", location2.getLocationId(), now.minusSeconds(11).toEpochMilli());
        PersonLocationData personLocationData6 = new PersonLocationData("1", location3.getLocationId(), now.minusSeconds(10).toEpochMilli());*/
        PersonLocationData personLocationData7 = new PersonLocationData("2", "123", now.minusSeconds(1).toEpochMilli());

        inputLocationDataTopic.pipeInput(location1.getLocationId(), location1, now.minusSeconds(19));
        inputLocationDataTopic.pipeInput(location2.getLocationId(), location2, now.minusSeconds(18));
        inputLocationDataTopic.pipeInput(location3.getLocationId(), location3, now.minusSeconds(17));
        inputLocationDataTopic.pipeInput(location4.getLocationId(), location4, now.minusSeconds(16));

        inputPersonLocationDataTopic.pipeInput(personLocationData1.getPersonId(), personLocationData1);
        inputPersonLocationDataTopic.pipeInput(personLocationData2.getPersonId(), personLocationData2);
        inputPersonLocationDataTopic.pipeInput(personLocationData3.getPersonId(), personLocationData3);
        inputPersonLocationDataTopic.pipeInput(personLocationData4.getPersonId(), personLocationData4);
        /*inputPersonLocationDataTopic.pipeInput(personLocationData5.getPersonId(), personLocationData5);
        inputPersonLocationDataTopic.pipeInput(personLocationData6.getPersonId(), personLocationData6);*/
        inputPersonLocationDataTopic.pipeInput(personLocationData7.getPersonId(), personLocationData7);

        Thread.sleep(15000);

        //TODO research how to right deterministic test
        KeyValue<Windowed<String>, SuspiciousPersonLocationAlert> stringSuspiciousPersonLocationAlertKeyValue = outputTopic.readKeyValue();
        assertNotNull(stringSuspiciousPersonLocationAlertKeyValue.value);
        assertTrue(stringSuspiciousPersonLocationAlertKeyValue.value.getPersonLocationDataList().size() > 1);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }
}