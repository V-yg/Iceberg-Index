### README

##### 概要

- 为存储在HDFS上的Iceberg表文件离线构建索引
- 表文件格式目前仅支持ORC
- 索引使用bloom-filter
- 索引存储格式包括默认反序列化格式以及puffin兼容格式（默认关闭，使用--puffin 开启）

##### 结构

```shell
tree src/main/java/org/example/
├── FileReader.java (retrieve ORC file from hdfs)
├── HDFS.java (wrapper for HDFS)
├── IndexBuilder.java (index generator)
├── Main.java (scan data files in given HDFS dir and generate index for them)
└── puffin
    ├── Blob.java
    ├── Footer.java
    ├── PuffinBuilder.java (transform blobs into puffin file)
    └── PuffinReader.java (get blobs from puffin file)
```



##### 使用

1. 编译

   ```
   mvn clean compile assembly:single
   ```

2. 运行

   ```
   java -classpath target/IndexTest-1.0-SNAPSHOT-jar-with-dependencies.jar org.example.Main \
   # hadoop-fs path, e.g 192.168.0.1:9000
   --hadoop_fs {{your path}} \
   # iceberg data file dir,e.g /iceberg-table-1655884530956/data
   --data_file_dir {{your dir}} \
   # false positive rate for bloom filter, e.g 0.001
   --fpp {{your rate}} \
   # enable puffin \
   --puffin
   ```

   