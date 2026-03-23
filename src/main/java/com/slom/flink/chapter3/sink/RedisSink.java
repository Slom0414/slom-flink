package com.slom.flink.chapter3.sink;

import com.slom.flink.chapter3.aggregate.UserOrderAgg;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class RedisSink extends RichSinkFunction<UserOrderAgg> {

    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String redisDomain = System.getenv("REDIS_DOMAIN");
        String redisPassword = System.getenv("REDIS_PASSWORD");

        jedis = new Jedis(redisDomain, 6379);
        jedis.auth(redisPassword);
    }

    @Override
    public void invoke(UserOrderAgg value, Context context) {
        if (value == null) {
            return;
        }
        jedis.hset("order_cnt",value.getUserId(),String.valueOf(value.getCount()));
    }


    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }
}