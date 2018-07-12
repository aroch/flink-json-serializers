package com.aroch.flink.serializer;

import com.aroch.flink.util.SerializerUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.function.Supplier;

@SuppressWarnings("WeakerAccess")
public class JacksonSerializer<T> extends TypeSerializer<T> {

    private final Class<T> type;
    private final ObjectMapper mapper;

    private transient final Supplier<T> objectSupplier;

    public JacksonSerializer(Class<T> type, ObjectMapper mapper, Supplier<T> objectSupplier) {
        this.type = type;
        this.mapper = mapper;
        this.objectSupplier = objectSupplier;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new JacksonSerializer<>(type, mapper, objectSupplier);
    }

    @Override
    public T createInstance() {
        return objectSupplier.get();
    }

    @Override
    public T copy(T from) {
        return mapper.convertValue(from, type);
    }

    @Override
    public T copy(T from, T reuse) {
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
    public void serialize(T t, DataOutputView dataOutputView) throws IOException {
        byte[] objectBytes = mapper.writeValueAsBytes(t);
        dataOutputView.writeInt(objectBytes.length);
        dataOutputView.write(objectBytes);
    }

    @Override
    public T deserialize(DataInputView dataInputView) throws IOException {
        return deserialize(null, dataInputView);
    }

    @Override
    public T deserialize(T reuse, DataInputView dataInputView) throws IOException {
        int frameSize = dataInputView.readInt();
        byte[] frame = new byte[frameSize];
        dataInputView.readFully(frame);
        return mapper.readValue(frame, type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj != null && obj.getClass() == JacksonSerializer.class) {
            final JacksonSerializer that = (JacksonSerializer) obj;
            return this.type == that.type;
        } else {
            return false;
        }
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj.getClass() == this.getClass();
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public TypeSerializerConfigSnapshot snapshotConfiguration() {
        return new JacksonSerializerConfigSnapshot<>(type);
    }

    @Override
    public CompatibilityResult<T> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
        if (configSnapshot instanceof JacksonSerializerConfigSnapshot) {
            return ((JacksonSerializerConfigSnapshot) configSnapshot).getTypeClass().equals(type) ? CompatibilityResult.compatible() : CompatibilityResult.requiresMigration();
        }
        return CompatibilityResult.requiresMigration();
    }

    @Override
    public String toString() {
        return getClass().getName() + " (" + type.getName() + ')';
    }

    @SuppressWarnings("unused")
    public static final class JacksonSerializerConfigSnapshot<T> extends GenericTypeSerializerConfigSnapshot {

        // used for reflection only
        public JacksonSerializerConfigSnapshot() {
        }

        JacksonSerializerConfigSnapshot(Class<T> typeClass) {
            super(typeClass);
        }

        @Override
        public int getVersion() {
            return 1;
        }
    }
}
