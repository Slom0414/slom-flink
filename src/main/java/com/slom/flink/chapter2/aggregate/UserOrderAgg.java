package com.slom.flink.chapter2.aggregate;

public class UserOrderAgg {

    public String userId;

    public int orderNum;

    public double amount;

    public UserOrderAgg(String userId, int orderNum, double amount) {
        this.userId = userId;
        this.orderNum = orderNum;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "UserOrderAgg{" +
                "userId='" + userId + '\'' +
                ", orderNum=" + orderNum +
                ", amount=" + amount +
                '}';
    }
}
