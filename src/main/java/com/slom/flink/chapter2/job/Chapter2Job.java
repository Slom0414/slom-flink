package com.slom.flink.chapter2.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slom.flink.chapter2.aggregate.ProvinceGmvAgg;
import com.slom.flink.chapter2.aggregate.UserOrderAgg;
import com.slom.flink.chapter2.event.OrderEvent;
import com.slom.flink.chapter2.event.ProvinceGmvEvent;
import com.slom.flink.chapter2.event.UserOrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Chapter2Job {

    public static final ObjectMapper MAPPER = new ObjectMapper();


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:19092")
                .setTopics("order_topic")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 定义两个FileSink代表有两个流输出到文件中
        FileSink<String> userOrderEventSink = FileSink
                .forRowFormat(
                        new Path("/opt/flink/chapter2/user/order"),
                        new SimpleStringEncoder<String>("UTF-8")
                )
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withInactivityInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build()
                )
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("user_order")
                                .withPartSuffix(".txt")
                                .build()
                )
                .build();

        FileSink<String> provinceGmvEventSink = FileSink
                .forRowFormat(
                        new Path("/opt/flink/chapter2/provinc/gmv"),
                        new SimpleStringEncoder<String>("UTF-8")
                )
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withInactivityInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build()
                )
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("province_gmv")
                                .withPartSuffix(".txt")
                                .build()
                )
                .build();

        // 定义侧输出流 outputTag
        OutputTag<UserOrderEvent> userOrderEventTag =
                new OutputTag<>("userOrderEvent") {};
        OutputTag<ProvinceGmvEvent> provinceGmvEventTag =
                new OutputTag<>("provinceGmvEvent") {};

        SingleOutputStreamOperator<Object> mainStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                // 转换算子1 map
                .map(orderData -> MAPPER.readValue(orderData, OrderEvent.class))
                // 转换算子2 过滤
                .filter(orderEvent -> orderEvent.amount > 0)
                // 转换算子3 flatmap 将单个orderEvent拆解为两个Event
                .flatMap((OrderEvent orderEvent, Collector<Object> collector) -> {
                    ProvinceGmvEvent provinceGmvEvent = new ProvinceGmvEvent();
                    UserOrderEvent userOrderEvent = new UserOrderEvent();

                    provinceGmvEvent.province = orderEvent.province;
                    provinceGmvEvent.ts = orderEvent.ts;
                    provinceGmvEvent.amount = orderEvent.amount;

                    userOrderEvent.orderId = orderEvent.orderId;
                    userOrderEvent.amount = orderEvent.amount;
                    userOrderEvent.ts = orderEvent.ts;
                    userOrderEvent.userId = orderEvent.userId;

                    collector.collect(userOrderEvent);
                    collector.collect(provinceGmvEvent);
                })
                .returns(Object.class)
                // 分流 将两个事件混合的流分流
                .process(new ProcessFunction<>() {
                    @Override
                    public void processElement(
                            Object event,
                            Context context,
                            Collector<Object> collector
                    ) {
                        if (event instanceof UserOrderEvent userOrderEvent) {
                            context.output(userOrderEventTag, userOrderEvent);
                        } else if (event instanceof ProvinceGmvEvent provinceGmvEvent) {
                            context.output(provinceGmvEventTag, provinceGmvEvent);
                        }
                    }
                });

        // 获得到两个流
        DataStream<UserOrderEvent> userOrderEventSideStream = mainStream.getSideOutput(userOrderEventTag);
        DataStream<ProvinceGmvEvent> provinceGmvEventSideStream = mainStream.getSideOutput(provinceGmvEventTag);

        // 聚合算子 reduce 输出sink
        userOrderEventSideStream
                .map(event -> new UserOrderAgg(event.userId, 1, event.amount))
                .keyBy(event -> event.userId)
                .reduce((a, b) -> new UserOrderAgg(
                        a.userId,
                        a.orderNum + b.orderNum,
                        a.amount + b.amount
                ))
                .map(agg -> agg.userId + "," + agg.orderNum + "," + agg.amount)
                .sinkTo(userOrderEventSink);

        // 同样reduce，先map 将event转为 agg， agg在去keyBy + reduce
        provinceGmvEventSideStream
                .map(event -> new ProvinceGmvAgg(event.province, event.amount))
                .keyBy(event -> event.province)
                .reduce((a, b) -> new ProvinceGmvAgg(
                        a.province,
                        a.amount + b.amount
                ))
                .map(agg -> agg.province + "," + agg.amount)
                .sinkTo(provinceGmvEventSink);

        env.execute("chapter2-job");
    }

}
