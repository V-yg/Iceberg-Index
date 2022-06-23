package org.example.puffin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.example.HDFS;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

public class PuffinReader {
    //    currently only support bloom filter
    static BloomFilter getBloomFilters(byte[] bytes) throws IOException {
        int length = bytes.length;
        int sizeDataOffset = length - 4 - 4 - 4;
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int payloadSize = byteBuffer.getInt(sizeDataOffset);
        byte[] payloadBuffer = new byte[payloadSize];
        System.arraycopy(bytes, sizeDataOffset - payloadSize, payloadBuffer, 0, payloadSize);
        String payloadJson = new String(payloadBuffer, StandardCharsets.UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        Footer footer = mapper.readValue(payloadJson, Footer.class);
        for (Blob blob : footer.blobs) {
            int dataLength = (int) blob.length;
            byte[] dataBuffer = new byte[dataLength];
            int dataOffset = (int) (blob.offset + 4);
            System.arraycopy(bytes, dataOffset, dataBuffer, 0, dataLength);
            BloomFilter<Long> bf = BloomFilter.readFrom(new ByteArrayInputStream(dataBuffer), Funnels.longFunnel());
            return bf;
        }
        throw new IOException("no filter found");
    }
}
