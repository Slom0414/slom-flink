package com.slom.kafka.databuilder.chapter3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaProducerExample {
    public static void main(String[] args) throws JsonProcessingException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-1:19092,kafka-2:19092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 批量发送相关配置
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 32768);          // 32KB
        props.put("linger.ms", 10);             // 等10ms凑一批
        props.put("buffer.memory", 33554432);   // 32MB
        props.put("compression.type", "lz4");   // 压缩，可改 snappy / gzip / zstd

        Producer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();
        Random random = new Random();

        int hotUserCount = 100;     // 热点用户数据条数
        int randomUserCount = 100;  // 随机用户数据条数

        try {
            // 第一批：热点用户 userId=1
            for (int i = 0; i < hotUserCount; i++) {
                OrderEvent orderEvent = new OrderEvent();
                orderEvent.ts = System.currentTimeMillis();
                orderEvent.province = "gd";
                orderEvent.orderId = String.valueOf(random.nextInt(100000,900000));
                orderEvent.amount = 100;
                orderEvent.userId = "1";

                ProducerRecord<String, String> msg = new ProducerRecord<>(
                        "order_topic",
                        orderEvent.userId,   // key = userId
                        mapper.writeValueAsString(orderEvent)
                );

                producer.send(msg).get();
            }

            // 第二批：随机用户 2~99
            for (int i = 0; i < randomUserCount; i++) {
                OrderEvent orderEvent1 = new OrderEvent();
                orderEvent1.ts = System.currentTimeMillis();
                orderEvent1.province = "gd";
                orderEvent1.orderId = String.valueOf(random.nextInt(100000,900000));
                orderEvent1.amount = 100;
                orderEvent1.userId = String.valueOf(random.nextInt(98) + 2);

                ProducerRecord<String, String> msg = new ProducerRecord<>(
                        "order_topic",
                        orderEvent1.userId,  // key = userId
                        mapper.writeValueAsString(orderEvent1)
                );

                producer.send(msg).get();
            }

            producer.flush();
            System.out.println("发送完成，总条数: " + (hotUserCount + randomUserCount));
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }
}