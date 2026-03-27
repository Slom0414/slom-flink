package com.slom.flink.chapterfinal.function;

import com.slom.flink.chapterfinal.delta.ChannelDelta;
import com.slom.flink.chapterfinal.event.OrderEvent;
import com.slom.flink.chapterfinal.state.OrderState;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;

public class OrderDeltaFlatMap extends RichFlatMapFunction<OrderEvent, ChannelDelta> {

    private transient ValueState<OrderState> orderStateValueState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<OrderState> descriptor =
                new ValueStateDescriptor<>("order-state", OrderState.class);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        descriptor.enableTimeToLive(ttlConfig);
        orderStateValueState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(OrderEvent event, Collector<ChannelDelta> out) throws Exception {
        // 1) hot delete 直接忽略
        if (isHotDelete(event)) {
            return;
        }

        OrderState state = orderStateValueState.value();
        if (state == null) {
            state = OrderState.empty(event.getId());
        }

        // 2) 忽略旧事件：event.updateTime < state.updateTime
        if (isOlderThanState(event, state)) {
            return;
        }

        String oldStatus = state.getStatus();
        BigDecimal oldAmount = nz(state.getAmount());

        // 3) 应用新事件
        state.apply(
                event.getChannel(),
                event.getAmount(),
                event.getStatus(),
                event.getCreateTime(),
                event.getFinishTime(),
                event.getUpdateTime()
        );

        String newStatus = state.getStatus();
        BigDecimal newAmount = nz(state.getAmount());

        ChannelDelta delta = null;

        if (oldStatus == null && isSuccess(newStatus)) {
            delta = new ChannelDelta();
            delta.setChannel(state.getChannel());
            delta.setAmountDelta(newAmount);
            delta.setSuccessCountDelta(1L);
        }

        // PROCESSING -> SUCCESS
        if (isProcessing(oldStatus) && isSuccess(newStatus)) {
            delta = new ChannelDelta();
            delta.setChannel(state.getChannel());
            delta.setAmountDelta(newAmount);
            delta.setSuccessCountDelta(1L);

            Long latency = calcLatencyMs(state);
            if (latency != null) {
                delta.setLatencyDelta(latency);
                delta.setLatencyCountDelta(1L);
            }
        }
        // SUCCESS -> FAILED
        else if (isSuccess(oldStatus) && isFailed(newStatus)) {
            delta = new ChannelDelta();
            delta.setChannel(state.getChannel());
            delta.setAmountDelta(oldAmount.negate());
            delta.setSuccessCountDelta(-1L);
        }
        // FAILED -> SUCCESS
        else if (isFailed(oldStatus) && isSuccess(newStatus)) {
            delta = new ChannelDelta();
            delta.setChannel(state.getChannel());
            delta.setAmountDelta(newAmount);
            delta.setSuccessCountDelta(1L);
        }
        // SUCCESS -> SUCCESS 金额修正
        else if (isSuccess(oldStatus) && isSuccess(newStatus)) {
            BigDecimal amountDelta = newAmount.subtract(oldAmount);
            if (amountDelta.compareTo(BigDecimal.ZERO) != 0) {
                delta = new ChannelDelta();
                delta.setChannel(state.getChannel());
                delta.setAmountDelta(amountDelta);
            }
        }

        // 4) 更新 state
        orderStateValueState.update(state);

        // 5) 输出 delta
        if (delta != null && !delta.isEmpty()) {
            out.collect(delta);
        }
    }

    private boolean isHotDelete(OrderEvent event) {
        return "hot".equalsIgnoreCase(event.getSrc()) && event.isDelete();
    }

    private boolean isOlderThanState(OrderEvent event, OrderState state) {
        LocalDateTime eventTime = event.getUpdateTime();
        LocalDateTime stateTime = state.getUpdateTime();

        if (eventTime == null || stateTime == null) {
            return false;
        }

        return eventTime.isBefore(stateTime);
    }

    private static boolean isProcessing(String status) {
        return "PROCESSING".equals(status);
    }

    private static boolean isSuccess(String status) {
        return "SUCCESS".equals(status);
    }

    private static boolean isFailed(String status) {
        return "FAILED".equals(status);
    }

    private static BigDecimal nz(BigDecimal v) {
        return v == null ? BigDecimal.ZERO : v;
    }

    private static Long calcLatencyMs(OrderState state) {
        if (state.getCreateTime() == null || state.getFinishTime() == null) {
            return null;
        }
        return Duration.between(state.getCreateTime(), state.getFinishTime()).toMillis();
    }
}