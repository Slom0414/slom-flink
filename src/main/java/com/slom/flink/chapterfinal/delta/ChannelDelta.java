package com.slom.flink.chapterfinal.delta;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.math.BigDecimal;

@Setter
@Getter
public class ChannelDelta implements Serializable {

    private String channel;
    private BigDecimal amountDelta;
    private Long successCountDelta;
    private Long latencyDelta;
    private Long latencyCountDelta;

    public ChannelDelta() {
    }

    public ChannelDelta(String channel,
                        BigDecimal amountDelta,
                        Long successCountDelta,
                        Long latencyDelta,
                        Long latencyCountDelta) {
        this.channel = channel;
        this.amountDelta = amountDelta;
        this.successCountDelta = successCountDelta;
        this.latencyDelta = latencyDelta;
        this.latencyCountDelta = latencyCountDelta;
    }

    public boolean isEmpty() {
        return amountDelta == null
                && successCountDelta == null
                && latencyDelta == null
                && latencyCountDelta == null;
    }
}