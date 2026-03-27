package com.slom.flink.chapterfinal.state;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;


@Setter
@Getter
public class OrderState implements Serializable {

    private Long id;
    private String channel;
    private BigDecimal amount;
    private String status;
    private LocalDateTime createTime;
    private LocalDateTime finishTime;
    private LocalDateTime updateTime;

    public static OrderState empty(Long id) {
        OrderState s = new OrderState();
        s.setId(id);
        s.setAmount(BigDecimal.ZERO);
        return s;
    }

    public void apply(String channel,
                      BigDecimal amount,
                      String status,
                      LocalDateTime createTime,
                      LocalDateTime finishTime,
                      LocalDateTime updateTime) {
        if (channel != null) {
            this.channel = channel;
        }
        if (amount != null) {
            this.amount = amount;
        }
        if (status != null) {
            this.status = status;
        }
        if (createTime != null) {
            this.createTime = createTime;
        }
        if (finishTime != null) {
            this.finishTime = finishTime;
        }
        if (updateTime != null) {
            this.updateTime = updateTime;
        }
    }


}