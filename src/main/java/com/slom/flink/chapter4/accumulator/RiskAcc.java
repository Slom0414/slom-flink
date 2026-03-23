package com.slom.flink.chapter4.accumulator;

public class RiskAcc {
    public int count;
    public double amountSum;

    public RiskAcc() {}

    public RiskAcc(int count, double amountSum) {
        this.count = count;
        this.amountSum = amountSum;
    }
}