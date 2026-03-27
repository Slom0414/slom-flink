package com.slom.flink.chapterfinal.event;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Setter
@Getter
public class OrderEvent implements Serializable {

    private String src;      // hot / cold
    private String opType;   // INSERT / UPDATE / DELETE
    private Long id;
    private String channel;
    private BigDecimal amount;
    private String status;
    private LocalDateTime createTime;
    private LocalDateTime finishTime;
    private LocalDateTime updateTime;


    public boolean isDelete() {
        return "DELETE".equals(opType);
    }
}