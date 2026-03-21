package com.slom.flink.chapter3.sink;

import com.slom.flink.chapter3.aggregate.UserOrderAgg;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RedisPipelineSink extends RichSinkFunction<UserOrderAgg> {

    private transient Jedis jedis;
    private transient Map<String, Long> bufferMap;

    private transient ScheduledExecutorService scheduler;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String redisDomain = System.getenv("REDIS_DOMAIN");
        String redisPassword = System.getenv("REDIS_PASSWORD");

        jedis = new Jedis(redisDomain, 6379);
        jedis.auth(redisPassword);
        bufferMap = new HashMap<>();

        // 创建定时任务线程池
        scheduler = Executors.newSingleThreadScheduledExecutor();

        // 每1秒执行一次
        scheduler.scheduleAtFixedRate(() -> {
            synchronized (this) {
                flushToRedis();
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void invoke(UserOrderAgg value, Context context) {
        if (value == null) {
            return;
        }

        synchronized (this) {
            bufferMap.put(value.getUserId(), value.getCount());
        }
    }

    private void flushToRedis() {
        if (bufferMap == null || bufferMap.isEmpty()) {
            return;
        }

        Pipeline pipeline = jedis.pipelined();
        for (Map.Entry<String, Long> entry : bufferMap.entrySet()) {
            pipeline.hset("order_cnt", entry.getKey(), String.valueOf(entry.getValue()));
        }
        pipeline.sync();

        bufferMap.clear();
    }

    @Override
    public void close() throws Exception {
        try {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }

            synchronized (this) {
                flushToRedis();
            }

        } finally {
            if (jedis != null) {
                jedis.close();
            }
            super.close();
        }
    }
}