package com.slom.flink.chapter1.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.slom.flink.chapter1.event.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class Chapter1Job {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000);


        // 定义 Kafka Source：只消费新到达的订单消息，消息值按字符串读取
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:19092")
                .setTopics("order_topic")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 定义 FileSink：将结果按行写入文件，并按时间/空闲时间/文件大小滚动生成新文件
        FileSink<String> fileSink = FileSink
                .forRowFormat(
                        new Path("/opt/flink/chapter1/output"),
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
                                .withPartPrefix("gmv")
                                .withPartSuffix(".txt")
                                .build()
                )
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
                // 从 Kafka 读取原始 JSON 字符串
                .map(json -> MAPPER.readValue(json, OrderEvent.class))
                // 反序列化为强类型订单对象，便于后续处理
                .map(Chapter1Job::format)
                // 转成最终输出的文本格式
                .sinkTo(fileSink);
        // 将结果持续写入文件

        env.execute("chapter1-job");
    }

    private static String format(OrderEvent e) {
        return e.province + "," + e.amount + "," + e.ts;
    }
}