package com.mleibman.common.model.order;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mleibman.common.model.KafkaIncomingData;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.math.BigDecimal;

@ToString
@EqualsAndHashCode
@Getter
public class OrderRewards implements KafkaIncomingData {

    private final long customerId;
    private final BigDecimal rewards;
    private final BigDecimal totalRewards;

    @JsonCreator
    public OrderRewards(@JsonProperty("customerId") long customerId,
                        @JsonProperty("rewards") BigDecimal rewards,
                        @JsonProperty("totalRewards") BigDecimal totalRewards) {
        this.customerId = customerId;
        this.rewards = rewards;
        this.totalRewards = totalRewards;
    }
}