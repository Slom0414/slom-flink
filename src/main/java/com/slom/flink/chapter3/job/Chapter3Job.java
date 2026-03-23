package com.slom.flink.chapter3.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slom.flink.chapter3.aggregate.UserOrderAgg;
import com.slom.flink.chapter3.event.OrderEvent;
import com.slom.flink.chapter3.sink.RedisSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.MathUtils;

public class Chapter3Job {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:19092")
                .setTopics("order_topic")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        RedisSink redisSink = new RedisSink();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                .map(orderData -> MAPPER.readValue(orderData, OrderEvent.class))
                // 第一阶段：加盐
                .map(orderEvent -> {
                    String saltedUserId;
                    if ("1".equals(orderEvent.userId)) {
                        int salt = Math.abs(
                                MathUtils.murmurHash(Integer.parseInt(orderEvent.orderId))
                        );
                        saltedUserId = orderEvent.userId + "_" + salt;
                    } else {
                        saltedUserId = orderEvent.userId + "_0";
                    }
                    return Tuple2.of(saltedUserId, 1L);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))

                // 第一阶段聚合：按加盐后做keyBy,此时基本上是数据均匀的
                .keyBy(t -> t.f0)
                .sum(1)

                // 第二阶段：已经得到计算结果了,去盐
                .map(t -> Tuple2.of(t.f0.split("_")[0], t.f1))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))

                // 第二阶段聚合：按真实 userId聚合结果
                .keyBy(t -> t.f0)
                .sum(1)

                // 转化为UserOrderAgg对象
                .map(t -> new UserOrderAgg(t.f0, t.f1))

                // 使用RedisSink
                .addSink(redisSink);

        env.execute("chapter3-job");
    }
}
