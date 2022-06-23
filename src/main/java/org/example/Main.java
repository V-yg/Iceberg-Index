package org.example;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.net.URISyntaxException;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Hello world!
 */
public class Main {


    public static void main(String[] args) throws ParseException, IOException, URISyntaxException {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("d", "data_file_dir", true, "directory for data file.")
                .addOption("h", "hadoop_fs", true, "hadoop fs path")
                .addOption("f", "fpp", true, "false positive probability for bloom filter.")
                .addOption("t", "test", false, "test flag")
                .addOption("p", "puffin", false, "use puffin as index file format");

        CommandLine line = parser.parse(options, args);
        String data_file_dir = line.getOptionValue("data_file_dir");
        String hadoop_fs = line.getOptionValue("hadoop_fs");
        double fpp = Double.parseDouble(line.getOptionValue("fpp", "0.01"));
        boolean puffin = line.hasOption("puffin");
        HDFS.init(hadoop_fs);

        List<String> dataFilePaths = HDFS.listDataFile(hadoop_fs, data_file_dir);
//        currently only support ORC file
        dataFilePaths = dataFilePaths.stream().filter(s -> s.endsWith(".orc")).collect(Collectors.toList());
//      build index for each data file in given directory
        for (String p : dataFilePaths) {
            IndexBuilder.newIndexBuilder().
                    fromDataFile(p).
                    usePuffin(puffin).
                    setIndexLocation().
                    setFPP(fpp).
                    buildBinaryIndex();
        }
    }
}
