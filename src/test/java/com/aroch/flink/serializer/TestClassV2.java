package com.aroch.flink.serializer;

import java.util.Objects;

public class TestClassV2 {

    public TestClassV2() {
    }

    public TestClassV2(Long time, String sId, String pgId, String type, String poop) {
        this.time = time;
        this.sId = sId;
        this.pgId = pgId;
        this.type = type;
        this.poop = poop;
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

    public String getPoop() {
        return poop;
    }

    public void setPoop(String poop) {
        this.poop = poop;
    }

    private Long time;
    private String sId;
    private String pgId;
    private String type;
    private String poop;

    @Override
    public String toString() {
        return "TestClassV1{" +
                "time=" + time +
                ", sId='" + sId + '\'' +
                ", pgId='" + pgId + '\'' +
                ", type='" + type + '\'' +
                ", poop='" + poop + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestClassV2 testClassV2 = (TestClassV2) o;
        return Objects.equals(time, testClassV2.time) &&
                Objects.equals(sId, testClassV2.sId) &&
                Objects.equals(pgId, testClassV2.pgId) &&
                Objects.equals(poop, testClassV2.poop) &&
                Objects.equals(type, testClassV2.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, sId, pgId, type, poop);
    }
}