package com.mleibman.common.streams.locationalert;

import com.mleibman.common.streams.CustomTimeExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Map;

@Slf4j
//@Configuration
public class LocationAlertKafkaStreamsConf {

    @Bean
    public KafkaStreamsConfiguration getKafkaStreamsConfiguration() {
        return new KafkaStreamsConfiguration(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "location_alerting_app_id_v1",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE,
                StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimeExtractor.class.getName()));
    }

    @Bean
    public FactoryBean<StreamsBuilder> streamBuilder(KafkaStreamsConfiguration streamsConfig) {
        StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean(streamsConfig);

        streamsBuilderFactoryBean.setInfrastructureCustomizer(new KafkaStreamsInfrastructureCustomizer() {
            @Override
            public void configureBuilder(StreamsBuilder builder) {

            }

            @Override
            public void configureTopology(Topology topology) {
                log.info("Topology: {}", topology.describe());
            }
        });

        return streamsBuilderFactoryBean;
    }

    @Bean
    public KStream<?, ?> kStream(LocationAlertKafkaStreamBuilder locationAlertKafkaStreamBuilder) {
        return locationAlertKafkaStreamBuilder
                .buildLocationAlertKafkaStream_GlobalKTable(new LocationAlertKafkaStreamProperties(60, 2));
    }
}