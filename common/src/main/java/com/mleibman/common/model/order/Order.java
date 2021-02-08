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
public class Order implements KafkaIncomingData {

    private final long orderId;
    private final long customerId;
    private final BigDecimal orderSum;

    @JsonCreator
    public Order(@JsonProperty("orderId") long orderId,
                 @JsonProperty("customerId") long customerId,
                 @JsonProperty("orderSum") BigDecimal orderSum) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.orderSum = orderSum;
    }
}