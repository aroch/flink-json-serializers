package com.aroch.flink.serializer;

import com.aroch.flink.util.SerializerUtils;
import com.fasterxml.jackson.databind.*;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

public class JsonNodeSerializer extends TypeSerializer<JsonNode> {

    private final ObjectReader reader;
    private final ObjectWriter writer;

    public JsonNodeSerializer() {
        this(new ObjectMapper());
    }

    public JsonNodeSerializer(ObjectMapper mapper) {
        this.reader = mapper.reader();
        this.writer = mapper.writer();
    }

    private JsonNodeSerializer(ObjectReader reader, ObjectWriter writer) {
        this.reader = reader;
        this.writer = writer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<JsonNode> duplicate() {
        return new JsonNodeSerializer(reader, writer);
    }

    @Override
    public JsonNode createInstance() {
        return reader.createObjectNode();
    }

    @Override
    public JsonNode copy(JsonNode from) {
        return from.deepCopy();
    }

    @Override
    public JsonNode copy(JsonNode from, JsonNode reuse) {
        return copy(from);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        SerializerUtils.copyFrame(source, target);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(JsonNode t, DataOutputView dataOutputView) throws IOException {
        byte[] objectBytes = writer.writeValueAsBytes(t);
        dataOutputView.writeInt(objectBytes.length);
        dataOutputView.write(objectBytes);
    }

    @Override
    public JsonNode deserialize(DataInputView dataInputView) throws IOException {
        return deserialize(null, dataInputView);
    }

    @Override
    public JsonNode deserialize(JsonNode reuse, DataInputView dataInputView) throws IOException {
        int frameSize = dataInputView.readInt();
        byte[] frame = new byte[frameSize];
        dataInputView.readFully(frame);
        return reader.readTree(new ByteArrayInputStream(frame));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JsonNodeSerializer that = (JsonNodeSerializer) o;
        return Objects.equals(reader.getConfig().getDeserializationFeatures(), that.reader.getConfig().getDeserializationFeatures()) &&
                Objects.equals(writer.getConfig().getSerializationFeatures(), that.writer.getConfig().getSerializationFeatures());
    }

    @Override
    public int hashCode() {
        return Objects.hash(reader, writer);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj.getClass() == this.getClass();
    }

    @Override
    public TypeSerializerConfigSnapshot snapshotConfiguration() {
        return new JsonNodeSerializerConfigSnapshot(reader.getConfig(), writer.getConfig());
    }

    @Override
    public CompatibilityResult<JsonNode> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
        if (configSnapshot instanceof JsonNodeSerializerConfigSnapshot) {
            int deserializationFeatures = reader.getConfig().getDeserializationFeatures();
            int serializationFeatures = writer.getConfig().getSerializationFeatures();
            boolean configsEqual = ((JsonNodeSerializerConfigSnapshot) configSnapshot).deserializationFeatures == deserializationFeatures &&
                    ((JsonNodeSerializerConfigSnapshot) configSnapshot).serializationFeatures == serializationFeatures;

            return configsEqual ? CompatibilityResult.compatible() : CompatibilityResult.requiresMigration();
        }
        return CompatibilityResult.requiresMigration();
    }

    @Override
    public String toString() {
        return "JsonNodeSerializer{" +
                "reader=" + reader +
                ", writer=" + writer +
                '}';
    }

    private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
        inputStream.defaultReadObject();
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.defaultWriteObject();
    }

    public static final class JsonNodeSerializerConfigSnapshot extends TypeSerializerConfigSnapshot {

        // bit-map representation of features
        private int deserializationFeatures;
        private int serializationFeatures;

        // used for reflection only
        public JsonNodeSerializerConfigSnapshot() {
        }

        JsonNodeSerializerConfigSnapshot(DeserializationConfig deserializationConfig, SerializationConfig serializationConfig) {
            this.deserializationFeatures = deserializationConfig.getDeserializationFeatures();
            this.serializationFeatures = serializationConfig.getSerializationFeatures();
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JsonNodeSerializerConfigSnapshot that = (JsonNodeSerializerConfigSnapshot) o;
            return Objects.equals(deserializationFeatures, that.deserializationFeatures) &&
                    Objects.equals(serializationFeatures, that.serializationFeatures);
        }

        @Override
        public int hashCode() {
            return Objects.hash(deserializationFeatures, serializationFeatures);
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            super.write(out);
            out.writeInt(deserializationFeatures);
            out.writeInt(serializationFeatures);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void read(DataInputView in) throws IOException {
            super.read(in);
            deserializationFeatures = in.readInt();
            serializationFeatures = in.readInt();
        }
    }
}
