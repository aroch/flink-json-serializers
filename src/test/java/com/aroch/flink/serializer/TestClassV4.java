package com.aroch.flink.serializer;

import java.util.Objects;

public class TestClassV4 {

    public TestClassV4() {
    }

    public TestClassV4(Long time, String sId, Long pgId) {
        this.time = time;
        this.sId = sId;
        this.pgId = pgId;
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

    public Long getPgId() {
        return pgId;
    }

    public void setPgId(Long pgId) {
        this.pgId = pgId;
    }

    private Long time;
    private String sId;
    private Long pgId;

    @Override
    public String toString() {
        return "TestClassV1{" +
                "time=" + time +
                ", sId='" + sId + '\'' +
                ", pgId='" + pgId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestClassV4 testClassV2 = (TestClassV4) o;
        return Objects.equals(time, testClassV2.time) &&
                Objects.equals(sId, testClassV2.sId) &&
                Objects.equals(pgId, testClassV2.pgId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, sId, pgId);
    }
}