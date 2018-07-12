package com.aroch.flink.serializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;
import java.util.function.Supplier;

public class JacksonSerializerTest extends SerializerTestBase<TestClassV1> {

    @Override
    protected TypeSerializer<TestClassV1> createSerializer() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return new JacksonSerializer<>(TestClassV1.class, mapper, new ClassInstanceSupplier());
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<TestClassV1> getTypeClass() {
        return TestClassV1.class;
    }

    @Override
    protected TestClassV1[] getTestData() {
        return new TestClassV1[]{
                new TestClassV1(1524732000000L, "abcd", "ddd", "session_started"),
                new TestClassV1(1524732000330L, "efgh", "ddd", "session_ended"),
                new TestClassV1(1524732000000L, "qwrt", "ddd", "session_started")
        };
    }

    private static class ClassInstanceSupplier implements Serializable, Supplier<TestClassV1> {

        @Override
        public TestClassV1 get() {
            return new TestClassV1();
        }
    }
}
