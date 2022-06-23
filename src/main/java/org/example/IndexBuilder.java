package org.example;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.example.puffin.PuffinBuilder;
import org.apache.orc.TypeDescription;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class IndexBuilder {
    String indexFilePath;
    String dataFilePath;
    double FPP;
    boolean usePuffin;

    static IndexBuilder newIndexBuilder() {
        return new IndexBuilder();
    }

    IndexBuilder fromDataFile(String dataFilePath) {
        this.dataFilePath = dataFilePath;
        return this;
    }

    IndexBuilder setIndexLocation(String location) {
        this.indexFilePath = location;
        return this;
    }

    IndexBuilder setFPP(double fpp) {
        this.FPP = fpp;
        return this;
    }

    IndexBuilder usePuffin(boolean puffin) {
        this.usePuffin = puffin;
        return this;
    }

    IndexBuilder setIndexLocation() {
        String[] split = dataFilePath.split("\\.");
        String suffix = usePuffin ? "puffin" : "idx";
        String path = String.join(".", split[0], suffix);
        return setIndexLocation(path);
    }

    void buildBinaryIndex() throws IOException {
        Reader reader = new FileReader(dataFilePath).read();
        long nrow = reader.getNumberOfRows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();

        try (RecordReader rows = reader.rows()) {
            BloomFilter<Long> bf = BloomFilter.create(Funnels.longFunnel(), nrow, FPP);

            while (rows.nextBatch(batch)) {
                LongColumnVector longVector = (LongColumnVector) batch.cols[0];
                Arrays.stream(longVector.vector).boxed().forEach(bf::put);
            }
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            bf.writeTo(byteArrayOutputStream);
            byte[] content = byteArrayOutputStream.toByteArray();

            if (usePuffin) {
                content = new PuffinBuilder().addBlob(content).build();
            }

            HDFS.writeTo(indexFilePath, content);
            System.out.printf("build bf index for %s(%d record) with size:%d\n", dataFilePath, nrow, content.length);
        }
    }

    static void checkIndex(Iterator<Long> iterator, String indexFilePath) throws IOException {
        FSDataInputStream index = HDFS.readFrom(indexFilePath);
        BloomFilter<Long> bf = BloomFilter.readFrom(index, Funnels.longFunnel());
        while (iterator.hasNext()) {
            Long id = iterator.next();
            boolean ok = bf.mightContain(id);
            if (!ok)
                throw new IOException();
        }
        System.out.printf("indexFile:%s with nums:%d passed \n", indexFilePath, 0);
    }

}
