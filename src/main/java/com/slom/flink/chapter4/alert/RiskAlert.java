package com.slom.flink.chapter4.alert;

public class RiskAlert {
    public String userId;
    public String ruleCode;
    public String ruleName;
    public long windowStart;
    public long windowEnd;
    public String detail;

    public RiskAlert() {}

    public RiskAlert(String userId, String ruleCode, String ruleName,
                     long windowStart, long windowEnd, String detail) {
        this.userId = userId;
        this.ruleCode = ruleCode;
        this.ruleName = ruleName;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.detail = detail;
    }

    @Override
    public String toString() {
        return "RiskAlert{" +
                "userId='" + userId + '\'' +
                ", ruleCode='" + ruleCode + '\'' +
                ", ruleName='" + ruleName + '\'' +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", detail='" + detail + '\'' +
                '}';
    }
}