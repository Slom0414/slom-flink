package com.slom.flink.chapterfinal.agg;

import lombok.Getter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Getter
public class ChannelAgg implements Serializable {

    private String channel;
    private BigDecimal totalAmount = BigDecimal.ZERO;
    private long successCount = 0L;
    private long totalLatencyMs = 0L;
    private long latencyCount = 0L;

    public void apply(com.slom.flink.chapterfinal.delta.ChannelDelta delta) {
        this.channel = delta.getChannel();

        if (delta.getAmountDelta() != null) {
            this.totalAmount = this.totalAmount.add(delta.getAmountDelta());
        }
        if (delta.getSuccessCountDelta() != null) {
            this.successCount += delta.getSuccessCountDelta();
        }
        if (delta.getLatencyDelta() != null) {
            this.totalLatencyMs += delta.getLatencyDelta();
        }
        if (delta.getLatencyCountDelta() != null) {
            this.latencyCount += delta.getLatencyCountDelta();
        }
    }

    public BigDecimal getAvgLatencyMs() {
        if (latencyCount <= 0) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(totalLatencyMs)
                .divide(BigDecimal.valueOf(latencyCount), 2, RoundingMode.HALF_UP);
    }


    @Override
    public String toString() {
        return "ChannelAgg{" +
                "channel='" + channel + '\'' +
                ", totalAmount=" + totalAmount +
                ", successCount=" + successCount +
                ", totalLatencyMs=" + totalLatencyMs +
                ", latencyCount=" + latencyCount +
                ", avgLatencyMs=" + getAvgLatencyMs() +
                '}';
    }
}