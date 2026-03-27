package com.slom.flink.chapterfinal.delta;

import lombok.Getter;

import java.io.Serializable;
import java.math.BigDecimal;


/**
 * 订单对渠道统计的贡献快照
 */
@Getter
public class Contribution implements Serializable {

    private String channel;
    private BigDecimal amount;
    private long count;
    private long latencyMs;

    public Contribution() {
    }

    public Contribution(String channel, BigDecimal amount, long count, long latencyMs) {
        this.channel = channel;
        this.amount = amount;
        this.count = count;
        this.latencyMs = latencyMs;
    }

    public static Contribution zero() {
        return new Contribution(null, BigDecimal.ZERO, 0L, 0L);
    }

    public boolean isZero() {
        return count == 0
                && (amount == null || amount.compareTo(BigDecimal.ZERO) == 0)
                && latencyMs == 0L;
    }
}