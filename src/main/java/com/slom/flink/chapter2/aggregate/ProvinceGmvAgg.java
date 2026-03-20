package com.slom.flink.chapter2.aggregate;

public class ProvinceGmvAgg {

    public String province;

    public double amount;

    public ProvinceGmvAgg(String province, double amount) {
        this.province = province;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "ProvinceGmvAgg{" +
                "province='" + province + '\'' +
                ", amount=" + amount +
                '}';
    }
}
