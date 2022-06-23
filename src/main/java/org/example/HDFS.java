package org.example;

import com.sun.jersey.core.util.StringIgnoreCaseKeyComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class HDFS {
    static FileSystem fs;
    static Configuration conf;

    public static void init(String hadoop_fs) throws IOException, URISyntaxException {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        fs = FileSystem.get(new URI(String.format("hdfs://%s", hadoop_fs)), conf);
    }

    static List<String> listDataFile(String hadoop_fs, String dir) throws IOException {
        String hadoop_dir_path = String.format("hdfs://%s%s", hadoop_fs, dir);

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(hadoop_dir_path), false);
        List<String> files = new ArrayList<>();
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            String fileName = next.getPath().getName();
            String hadoop_file_path = String.format("hdfs://%s%s", hadoop_fs, Paths.get(dir, fileName));
            files.add(hadoop_file_path);
        }
        return files;
    }

    static void writeTo(String path, byte[] content) throws IOException {
        try (FSDataOutputStream outputStream = fs.create(new Path(path))) {
            outputStream.write(content);
        }
    }

    static FSDataInputStream readFrom(String path) throws IOException {
        return fs.open(new Path(path));
    }
}
