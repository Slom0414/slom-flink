package com.slom.flink.chapterfinal.function;

import com.slom.flink.chapterfinal.agg.ChannelAgg;
import com.slom.flink.chapterfinal.delta.ChannelDelta;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;

public class ChannelAggFlatMap extends RichFlatMapFunction<ChannelDelta, ChannelAgg> {

    private transient ValueState<ChannelAgg> aggState;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        ValueStateDescriptor<ChannelAgg> descriptor =
                new ValueStateDescriptor<>("channel-agg", ChannelAgg.class);
        aggState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(ChannelDelta delta, Collector<ChannelAgg> out) throws Exception {
        ChannelAgg agg = aggState.value();
        if (agg == null) {
            agg = new ChannelAgg();
        }

        agg.apply(delta);
        aggState.update(agg);

        out.collect(agg);
    }
}