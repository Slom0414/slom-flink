package com.slom.flink.chapter4.event;

public class PayEvent {
    public String userId;
    public double amount;
    public String ip;
    public String country;
    public long eventTime;

    public PayEvent() {}

    public PayEvent(String userId, double amount, String ip, String country, long eventTime) {
        this.userId = userId;
        this.amount = amount;
        this.ip = ip;
        this.country = country;
        this.eventTime = eventTime;
    }
}