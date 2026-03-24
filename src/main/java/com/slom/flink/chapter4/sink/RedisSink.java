package com.slom.flink.chapter4.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slom.flink.chapter4.alert.RiskAlert;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

@Slf4j
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
        objectMapper = new ObjectMapper();

        log.info("[RISK_DEBUG][SINK_OPEN] redis ping={}", jedis.ping());
    }

    @Override
    public void invoke(RiskAlert value, Context context) throws JsonProcessingException {
        if (value == null) {
            log.info("[RISK_DEBUG][SINK] skip null value");
            return;
        }

        String json = objectMapper.writeValueAsString(value);
        log.info("[RISK_DEBUG][SINK] writing key={} value={}", "risk::" + value.userId, json);

        SetParams setParams = new SetParams();
        setParams.ex(60 * 60 * 24);

        jedis.set("risk::" + value.userId, json, setParams);

        log.info("[RISK_DEBUG][SINK] write success key={}", "risk::" + value.userId);
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }
}