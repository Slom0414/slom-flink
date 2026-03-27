package com.slom.flink.chapter4.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slom.flink.chapter4.accumulator.RiskAcc;
import com.slom.flink.chapter4.alert.RiskAlert;
import com.slom.flink.chapter4.event.PayEvent;
import com.slom.flink.chapter4.sink.RedisSink;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
@Slf4j
public class Chapter4Job {

    public static void main(String[] args) throws Exception {

        log.info("[RISK_DEBUG][JOB] chapter4-job starting...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        ParameterTool pt = ParameterTool.fromArgs(args);
        String redisDomain = pt.getRequired("redis.domain");
        String redisPassword = pt.getRequired("redis.password");

        log.info("[RISK_DEBUG][JOB] redisDomain={}", redisDomain);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:19092")
                .setTopics("pay_topic")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<PayEvent> wm =
                WatermarkStrategy
                        .<PayEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.eventTime)
                        // 这个后面会讲解，现在先不需要了解
                        .withIdleness(Duration.ofSeconds(10));

        ObjectMapper objectMapper = new ObjectMapper();

        SingleOutputStreamOperator<PayEvent> payEventStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "pay-source")
                .map(json -> {
                    log.info("[RISK_DEBUG][SOURCE] rawJson={}", json);
                    PayEvent event = objectMapper.readValue(json, PayEvent.class);
                    log.info(
                            "[RISK_DEBUG][PARSE] userId={} amount={} ip={} country={} eventTime={}",
                            event.userId,
                            event.amount,
                            event.ip,
                            event.country,
                            event.eventTime
                    );
                    return event;
                })
                .assignTimestampsAndWatermarks(wm)
                .process(new ProcessFunction<PayEvent, PayEvent>() {
                    @Override
                    public void processElement(PayEvent value, Context ctx, Collector<PayEvent> out) {
                        log.info(
                                "[RISK_DEBUG][WM] userId={} eventTime={} currentWatermark={}",
                                value.userId,
                                value.eventTime,
                                ctx.timerService().currentWatermark()
                        );
                        out.collect(value);
                    }
                });

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

                                log.info(
                                        "[RISK_DEBUG][AGG] userId={} eventTime={} count={} amountSum={}",
                                        value.userId,
                                        value.eventTime,
                                        acc.count,
                                        acc.amountSum
                                );

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
                        new ProcessWindowFunction<RiskAcc, RiskAlert, String, TimeWindow>() {
                            @Override
                            public void process(
                                    String userId,
                                    ProcessWindowFunction<RiskAcc, RiskAlert, String, TimeWindow>.Context context,
                                    Iterable<RiskAcc> iterable,
                                    Collector<RiskAlert> out
                            ) throws Exception {

                                RiskAcc acc = iterable.iterator().next();

                                log.info(
                                        "[RISK_DEBUG][WINDOW] userId={} windowStart={} windowEnd={} acc={}",
                                        userId,
                                        context.window().getStart(),
                                        context.window().getEnd(),
                                        objectMapper.writeValueAsString(acc)
                                );


                                if (acc.count >= 3) {
                                    RiskAlert alert = new RiskAlert(
                                            userId,
                                            "RULE_1",
                                            "10秒内支付次数>=3",
                                            context.window().getStart(),
                                            context.window().getEnd(),
                                            "count=" + acc.count
                                    );

                                    log.info(
                                            "[RISK_DEBUG][ALERT] hit RULE_1 userId={} windowStart={} windowEnd={} detail={}",
                                            userId,
                                            context.window().getStart(),
                                            context.window().getEnd(),
                                            alert.detail
                                    );

                                    out.collect(alert);
                                }

                                if (acc.amountSum > 5000) {
                                    RiskAlert alert = new RiskAlert(
                                            userId,
                                            "RULE_2",
                                            "10秒内总金额>5000",
                                            context.window().getStart(),
                                            context.window().getEnd(),
                                            "amountSum=" + acc.amountSum
                                    );

                                    log.info(
                                            "[RISK_DEBUG][ALERT] hit RULE_2 userId={} windowStart={} windowEnd={} detail={}",
                                            userId,
                                            context.window().getStart(),
                                            context.window().getEnd(),
                                            alert.detail
                                    );

                                    out.collect(alert);
                                }
                            }
                        }
                )
                .addSink(new RedisSink(redisDomain, redisPassword));

        env.execute("chapter4-job");
    }
}