package com.slom.flink.chapter4.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slom.flink.chapter3.aggregate.UserOrderAgg;
import com.slom.flink.chapter4.alert.RiskAlert;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;


public class RedisSink extends RichSinkFunction<RiskAlert> {

    private transient Jedis jedis;

    private transient ObjectMapper objectMapper;

    private final String redisDomain;

    private final String redisPassword;

    public RedisSink(String redisDomain, String redisPassword) {
        this.redisDomain = redisDomain;
        this.redisPassword = redisPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);



        jedis = new Jedis(redisDomain, 6379);
        jedis.auth(redisPassword);
    }

    @Override
    public void invoke(RiskAlert value, Context context) throws JsonProcessingException {
        if (value == null) {
            return;
        }

        SetParams setParams = new SetParams();
        // 风控1天
        setParams.ex(60 * 60 * 24);
        jedis.set("risk::"+value.userId, objectMapper.writeValueAsString(value),setParams);

    }


    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }
}