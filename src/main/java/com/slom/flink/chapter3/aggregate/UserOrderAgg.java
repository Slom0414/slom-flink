package com.slom.flink.chapter3.aggregate;

import java.io.Serializable;

public class UserOrderAgg implements Serializable {
    private String userId;
    private long count;

    public UserOrderAgg() {
    }

    public UserOrderAgg(String userId, long count) {
        this.userId = userId;
        this.count = count;
    }

    public String getUserId() {
        return userId;
    }

    public long getCount() {
        return count;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UserOrderAgg{" +
                "userId='" + userId + '\'' +
                ", count=" + count +
                '}';
    }
}