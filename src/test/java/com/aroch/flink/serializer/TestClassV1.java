package com.aroch.flink.serializer;

import java.util.Objects;

public class TestClassV1 {

    public TestClassV1() {
    }

    public TestClassV1(Long time, String sId, String pgId, String type) {
        this.time = time;
        this.sId = sId;
        this.pgId = pgId;
        this.type = type;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getsId() {
        return sId;
    }

    public void setsId(String sId) {
        this.sId = sId;
    }

    public String getPgId() {
        return pgId;
    }

    public void setPgId(String pgId) {
        this.pgId = pgId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    private Long time;
    private String sId;
    private String pgId;
    private String type;

    @Override
    public String toString() {
        return "TestClassV1{" +
                "time=" + time +
                ", sId='" + sId + '\'' +
                ", pgId='" + pgId + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestClassV1 testClassV1 = (TestClassV1) o;
        return Objects.equals(time, testClassV1.time) &&
                Objects.equals(sId, testClassV1.sId) &&
                Objects.equals(pgId, testClassV1.pgId) &&
                Objects.equals(type, testClassV1.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, sId, pgId, type);
    }
}