package com.mleibman.common.streams.order;

import com.mleibman.common.model.order.Order;
import com.mleibman.common.model.order.OrderRewards;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Slf4j
@Service
public class OrderRewardsKafkaStreamBuilder {

    private final StreamsBuilder streamBuilder;

    @Autowired
    public OrderRewardsKafkaStreamBuilder(StreamsBuilder streamBuilder) {
        this.streamBuilder = streamBuilder;
    }

    public KStream<String, Order> buildOrderRewardsKafkaStream() {

        KStream<String, Order> orderStream = streamBuilder.stream("ORDER-DATA",
                Consumed.with(Serdes.String(), new JsonSerde<>(Order.class)));

        streamBuilder.addStateStore(Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("OrderRewardsStore"),
                Serdes.Long(), new JsonSerde<>(BigDecimal.class)));

        orderStream.transformValues(OrderRewardsTransformer::new, "OrderRewardsStore")
                .peek((key, message) -> log.info("Sending order reward {}", message))
                .to("ORDER-REWARDS-DATA", Produced.with(Serdes.String(), new JsonSerde<>(OrderRewards.class)));

        return orderStream;
    }
}