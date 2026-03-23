package com.slom.flink.chapter4.job;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.slom.flink.chapter4.accumulator.RiskAcc;
import com.slom.flink.chapter4.alert.RiskAlert;
import com.slom.flink.chapter4.event.PayEvent;
import com.slom.flink.chapter4.sink.RedisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 交易风控系统
 * 规则 1：短时间内高频支付
 * 同一用户 10秒内支付 ≥ 3 次 → 标记为风险用户
 * 规则 2：金额异常
 * 10秒内总金额 > 5000 → 风险
 * 规则 3：乱序数据必须正确处理
 * 规则 4：迟到数据补偿
 */
public class Chapter4Job {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        ParameterTool pt = ParameterTool.fromArgs(args);
        String redisDomain = pt.getRequired("redis.domain");
        String redisPassword = pt.getRequired("redis.password");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:19092")
                .setTopics("pay_topic")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 规则4 乱序数据必须正确处理 必然是需要固定延迟水位线的,并且timestamp使用eventTime

        WatermarkStrategy<PayEvent> wm =
                WatermarkStrategy
                        // 最大延迟5s
                        .<PayEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.eventTime);


        ObjectMapper objectMapper = new ObjectMapper();

        SingleOutputStreamOperator<PayEvent> payEventStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "pay-source")
                .map(json -> objectMapper.readValue(json, PayEvent.class))
                .assignTimestampsAndWatermarks(wm);

        // 规则1 同一用户 10秒内支付 ≥ 3 次 → 标记为风险用户
        // 按key分组,然后再进行滑动窗口,size = 10, 滑动步长是 5,几乎不漏减（除了这种情况 4.9 5.0 14.9）
        payEventStream
                .keyBy(payEvent -> payEvent.userId)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .aggregate(
                        new AggregateFunction<PayEvent, RiskAcc, RiskAcc>() {
                            @Override
                            public RiskAcc createAccumulator() {
                                return new RiskAcc(0, 0.0);
                            }

                            @Override
                            public RiskAcc add(PayEvent value, RiskAcc acc) {
                                acc.count += 1;
                                acc.amountSum += value.amount;
                                return acc;
                            }

                            @Override
                            public RiskAcc getResult(RiskAcc acc) {
                                return acc;
                            }

                            @Override
                            public RiskAcc merge(RiskAcc a, RiskAcc b) {
                                return new RiskAcc(a.count + b.count, a.amountSum + b.amountSum);
                            }
                        },
                        new ProcessWindowFunction<RiskAcc,RiskAlert,String,TimeWindow>() {
                            @Override
                            public void process(String userId, ProcessWindowFunction<RiskAcc, RiskAlert, String, TimeWindow>.Context context,
                                                Iterable<RiskAcc> iterable, Collector<RiskAlert> out) throws Exception {
                                RiskAcc acc = iterable.iterator().next();

                                if (acc.count >= 3) {
                                    out.collect(new RiskAlert(
                                            userId,
                                            "RULE_1",
                                            "10秒内支付次数>=3",
                                            context.window().getStart(),
                                            context.window().getEnd(),
                                            "count=" + acc.count
                                    ));
                                }

                                if (acc.amountSum > 5000) {
                                    out.collect(new RiskAlert(
                                            userId,
                                            "RULE_2",
                                            "10秒内总金额>5000",
                                            context.window().getStart(),
                                            context.window().getEnd(),
                                            "amountSum=" + acc.amountSum
                                    ));
                                }
                            }

                        }
                ).addSink(new RedisSink(redisDomain, redisPassword));

    }
}
