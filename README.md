## README

### 概要

通过离线作业，为存储在HDFS上的Iceberg数据文件构建索引

- 表文件格式目前支持ORC类型
- 索引使用bloom-filter
- 索引存储格式包括默认反序列化格式及puffin兼容格式（默认关闭，使用--puffin 开启）

### 结构

```shell
tree src/main/java/org/example/
├── FileReader.java (从HDFS读ORC文件)
├── HDFS.java (HDFS配置包装类)
├── IndexBuilder.java (代码主要部分，负责根据列数据生成索引)
├── Main.java (扫描指定目录下的数据文件，并构建索引)
└── puffin
    ├── Blob.java
    ├── Footer.java
    ├── PuffinBuilder.java (将BLOB（序列化后的bloom-filter）转换为puffin格式)
    └── PuffinReader.java (从puffin文件恢复bloom-filter)
```



### 使用

1. 编译

   ```
   mvn clean compile assembly:single
   ```

2. 运行（需要用户指定hdfs路径、iceberg数据目录等）

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

3. 预期结果

   程序会扫描用户指定目录下的所有iceberg数据文件，并为每个数据文件在同一目录下生成对应的索引文件

### 其他

- 兼容性

  目前没有太多考虑兼容问题，文件格式只支持ORC，索引类型只支持bloom-filter。随着后面进展可能再重构

- 索引列选择

  索引列选择也没有考虑兼容问题，代码实现中模拟主键建索引的情况，假定第一列为Long类型，选择第一列作为索引





