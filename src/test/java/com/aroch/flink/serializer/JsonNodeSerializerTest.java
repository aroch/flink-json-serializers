package com.aroch.flink.serializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class JsonNodeSerializerTest extends SerializerTestBase<JsonNode> {

    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    protected TypeSerializer<JsonNode> createSerializer() {

        return new JsonNodeSerializer(mapper);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<JsonNode> getTypeClass() {
        return JsonNode.class;
    }

    @Override
    protected JsonNode[] getTestData() {
        return new JsonNode[]{
                mapper.createObjectNode().put("time", 1524732000000L).put("type", "session_started"),
                mapper.createObjectNode().put("time", 1524732001000L).put("type", "session_started"),
                mapper.createObjectNode().put("time", 1524732002000L).put("type", "session_started")
        };
    }
}
