package org.example.puffin;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PuffinBuilder {
    final static byte[] MAGIC = {0x50, 0x46, 0x41, 0x31};
    final static int NO_COMPRESSION_FLAG = 0;
    public static final String BF_TYPE = "BLOOM-FILTER";
    public static final List<Integer> BF_FIELD = new ArrayList<>(Collections.singletonList(0));
    public static ObjectMapper objectMapper = new ObjectMapper();


    public List<Blob> blobs = new ArrayList<>();
    Footer footer = new Footer(blobs);

    int offset = 0;
    boolean compression;

    byte[] getFooter() throws JsonProcessingException {
        ByteBuffer buffer = ByteBuffer.allocate(1000).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(MAGIC);
        byte[] payload = footer.toJson().getBytes(StandardCharsets.UTF_8);
        buffer.put(payload);
        buffer.putInt(payload.length);
        buffer.putInt(NO_COMPRESSION_FLAG);
        buffer.put(MAGIC);
        System.out.printf("parload%s,len:%d\n", footer.toJson(), payload.length);
        buffer.flip();
        byte[] remaining = new byte[buffer.remaining()];
        buffer.get(remaining);
        return remaining;
    }

    public PuffinBuilder addBlob(byte[] content) {
        return addBlob(content, BF_TYPE, BF_FIELD);
    }

    public PuffinBuilder addBlob(byte[] content, String type, List<Integer> fields) {
        Blob blob = new Blob(type, fields, content, offset);
        offset += content.length;
        blobs.add(blob);
        return this;
    }

    public PuffinBuilder setCompressed(boolean compressed) {
        this.compression = compressed;
        return this;
    }


    public byte[] build() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate((offset * 3)).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(MAGIC);
        blobs.stream().map(Blob::content).forEach(buffer::put);
        buffer.put(getFooter());
        buffer.flip();
        byte[] remaining = new byte[buffer.remaining()];
        buffer.get(remaining);
        return remaining;
    }
}
