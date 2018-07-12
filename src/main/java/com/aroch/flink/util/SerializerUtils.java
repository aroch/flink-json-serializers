package com.aroch.flink.util;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class SerializerUtils {

    public static void copyFrame(DataInputView source, DataOutputView target) throws IOException {
        int frameSize = source.readInt();
        byte[] frame = new byte[frameSize];
        source.readFully(frame);
        target.writeInt(frameSize);
        target.write(frame);
    }
}
