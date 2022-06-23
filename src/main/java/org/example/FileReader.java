package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import java.io.IOException;

public class FileReader {
    String filePath;
    public FileReader(String filePath) {
        this.filePath = filePath;
    }

    public Reader read() throws IOException {
        return OrcFile.createReader(new Path(filePath),
                OrcFile.readerOptions(HDFS.conf));
    }
}
