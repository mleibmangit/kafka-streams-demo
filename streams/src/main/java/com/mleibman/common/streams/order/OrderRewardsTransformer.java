package com.mleibman.common.streams.order;

import com.mleibman.common.model.order.Order;
import com.mleibman.common.model.order.OrderRewards;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;

public class OrderRewardsTransformer implements ValueTransformer<Order, OrderRewards> {

    private KeyValueStore<Long, BigDecimal> orderRewardsKeyValueStore;

    @Override
    public void init(ProcessorContext context) {
        orderRewardsKeyValueStore = (KeyValueStore<Long, BigDecimal>) context.getStateStore("OrderRewardsStore");
    }

    @Override
    public OrderRewards transform(Order order) {

        BigDecimal totalRewards;
        BigDecimal currentTotalRewards = orderRewardsKeyValueStore.get(order.getCustomerId());
        BigDecimal rewardsForOrder = order.getOrderSum().divide(BigDecimal.valueOf(10));

        if (currentTotalRewards == null) {
            totalRewards = rewardsForOrder;
        } else {
            totalRewards = currentTotalRewards.add(rewardsForOrder);
        }

        orderRewardsKeyValueStore.put(order.getCustomerId(), totalRewards);
        return new OrderRewards(order.getCustomerId(), rewardsForOrder, totalRewards);
    }

    @Override
    public void close() {

    }
}
