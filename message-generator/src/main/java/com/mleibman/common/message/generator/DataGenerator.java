package com.mleibman.common.message.generator;

import com.mleibman.common.message.generator.kafka.KafkaSender;
import com.mleibman.common.model.Location;
import com.mleibman.common.model.PersonLocationData;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.UUID;

@Slf4j
@Component
public class DataGenerator {

    private final KafkaSender kafkaSender;

    @Autowired
    public DataGenerator(KafkaSender kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @Scheduled(fixedRate = 5000)
    public void generateLocationData() {
        log.info("The time is now {}", LocalDate.now());
        Location location = generateRandomLocation();
        kafkaSender.send("SUSPICIOUS-LOCATION-DATA", location.getLocationId(), location);
    }

    @Scheduled(fixedRate = 5000)
    public void generatePersonLocationData() {
        log.info("The time is now {}", LocalDate.now());
        PersonLocationData personLocationData = generateRandomPersonLocation();
        kafkaSender.send("PERSON-LOCATION-DATA", personLocationData.getPersonId(), personLocationData);
    }

    public static PersonLocationData generateRandomPersonLocation() {
        return new PersonLocationData(UUID.randomUUID().toString(), generateRandomLocation(), System.currentTimeMillis());
    }

    public static Location generateRandomLocation() {
        return new Location("test", getRandomLong(), getRandomLong());
    }

    private static long getRandomLong() {
        return new RandomDataGenerator().nextLong(100, 300);
    }
}