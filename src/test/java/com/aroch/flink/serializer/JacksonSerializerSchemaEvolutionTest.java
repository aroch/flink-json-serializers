package com.aroch.flink.serializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.function.Supplier;

public class JacksonSerializerSchemaEvolutionTest {


    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void noChangeTest() throws IOException {

        JacksonSerializer<TestClassV1> jacksonSerializerV1 = new JacksonSerializer<>(TestClassV1.class, mapper, new ClassInstanceSupplierV1());

        TestClassV1 inputObject = new TestClassV1();
        inputObject.setPgId("some_pgid");
        inputObject.setsId("some_sid");
        inputObject.setTime(1529502874413L);
        inputObject.setType("some_type");

        TestClassV1 expectedObject = new TestClassV1();
        expectedObject.setPgId("some_pgid");
        expectedObject.setsId("some_sid");
        expectedObject.setTime(1529502874413L);
        expectedObject.setType("some_type");

        Object outputObject = serDeser(jacksonSerializerV1, jacksonSerializerV1, inputObject);

        Assert.assertEquals(expectedObject, outputObject);
    }

    @Test
    public void fieldAddedTest() throws IOException {

        JacksonSerializer<TestClassV1> jacksonSerializerV1 = new JacksonSerializer<>(TestClassV1.class, mapper, new ClassInstanceSupplierV1());
        JacksonSerializer<TestClassV2> jacksonSerializerV2 = new JacksonSerializer<>(TestClassV2.class, mapper, new ClassInstanceSupplierV2());

        TestClassV1 inputObject = new TestClassV1();
        inputObject.setPgId("some_pgid");
        inputObject.setsId("some_sid");
        inputObject.setTime(1529502874413L);
        inputObject.setType("some_type");

        TestClassV2 expectedObject = new TestClassV2();
        expectedObject.setPgId("some_pgid");
        expectedObject.setsId("some_sid");
        expectedObject.setTime(1529502874413L);
        expectedObject.setType("some_type");
        expectedObject.setPoop(null);

        Object outputObject = serDeser(jacksonSerializerV1, jacksonSerializerV2, inputObject);

        Assert.assertEquals(expectedObject, outputObject);
    }

    @Test
    public void fieldRemovedTest() throws IOException {

        JacksonSerializer<TestClassV1> jacksonSerializerV1 = new JacksonSerializer<>(TestClassV1.class, mapper, new ClassInstanceSupplierV1());
        JacksonSerializer<TestClassV3> jacksonSerializerV3 = new JacksonSerializer<>(TestClassV3.class, mapper, new ClassInstanceSupplierV3());

        TestClassV1 inputObject = new TestClassV1();
        inputObject.setPgId("some_pgid");
        inputObject.setsId("some_sid");
        inputObject.setTime(1529502874413L);
        inputObject.setType("some_type");

        TestClassV3 expectedObject = new TestClassV3();
        expectedObject.setPgId("some_pgid");
        expectedObject.setsId("some_sid");
        expectedObject.setTime(1529502874413L);

        Object outputObject = serDeser(jacksonSerializerV1, jacksonSerializerV3, inputObject);

        Assert.assertEquals(expectedObject, outputObject);
    }

    @Test
    public void fieldTypeConversionTest() throws IOException {

        JacksonSerializer<TestClassV1> jacksonSerializerV1 = new JacksonSerializer<>(TestClassV1.class, mapper, new ClassInstanceSupplierV1());
        JacksonSerializer<TestClassV4> jacksonSerializerV4 = new JacksonSerializer<>(TestClassV4.class, mapper, new ClassInstanceSupplierV4());

        TestClassV1 inputObject = new TestClassV1();
        inputObject.setPgId("1234567");
        inputObject.setsId("some_sid");
        inputObject.setTime(1529502874413L);
        inputObject.setType("some_type");

        TestClassV4 expectedObject = new TestClassV4();
        expectedObject.setPgId(1234567L);
        expectedObject.setsId("some_sid");
        expectedObject.setTime(1529502874413L);

        Object outputObject = serDeser(jacksonSerializerV1, jacksonSerializerV4, inputObject);

        Assert.assertEquals(expectedObject, outputObject);
    }

    @Test(expected = InvalidFormatException.class)
    public void fieldIllegalTypeConversionTest() throws IOException {

        JacksonSerializer<TestClassV1> jacksonSerializerV1 = new JacksonSerializer<>(TestClassV1.class, mapper, new ClassInstanceSupplierV1());
        JacksonSerializer<TestClassV4> jacksonSerializerV4 = new JacksonSerializer<>(TestClassV4.class, mapper, new ClassInstanceSupplierV4());

        TestClassV1 inputObject = new TestClassV1();
        inputObject.setPgId("not_a_number");
        inputObject.setsId("some_sid");
        inputObject.setTime(1529502874413L);
        inputObject.setType("some_type");

        TestClassV4 expectedObject = new TestClassV4();
        expectedObject.setPgId(1234567L);
        expectedObject.setsId("some_sid");
        expectedObject.setTime(1529502874413L);

        // Expecting an exception to be thrown
        serDeser(jacksonSerializerV1, jacksonSerializerV4, inputObject);
    }

    private Object serDeser(JacksonSerializer serializer, JacksonSerializer deserializer, Object testObject) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serializer.serialize(testObject, new MockOutput(out));
        return deserializer.deserialize(new MockInput(new ByteArrayInputStream(out.toByteArray())));
    }

    private static class MockInput extends DataInputStream implements DataInputView {

        public MockInput(InputStream in) {
            super(in);
        }

        @Override
        public void skipBytesToRead(int numBytes) throws IOException {
        }
    }

    private static class MockOutput extends DataOutputStream implements DataOutputView {

        public MockOutput(OutputStream out) {
            super(out);
        }

        @Override
        public void skipBytesToWrite(int numBytes) throws IOException {
        }

        @Override
        public void write(DataInputView source, int numBytes) throws IOException {

        }
    }

    private static class ClassInstanceSupplierV1 implements Serializable, Supplier<TestClassV1> {

        @Override
        public TestClassV1 get() {
            return new TestClassV1();
        }
    }

    private static class ClassInstanceSupplierV2 implements Serializable, Supplier<TestClassV2> {

        @Override
        public TestClassV2 get() {
            return new TestClassV2();
        }
    }

    private static class ClassInstanceSupplierV3 implements Serializable, Supplier<TestClassV3> {

        @Override
        public TestClassV3 get() {
            return new TestClassV3();
        }
    }

    private static class ClassInstanceSupplierV4 implements Serializable, Supplier<TestClassV4> {

        @Override
        public TestClassV4 get() {
            return new TestClassV4();
        }
    }
}
