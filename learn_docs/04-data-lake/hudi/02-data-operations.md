# Apache Hudi 数据操作

## 目录

- [快速开始](#快速开始)
- [Flink 集成](#flink-集成)
- [Spark 集成](#spark-集成)
- [增量处理](#增量处理)
- [最佳实践](#最佳实践)

---

## 快速开始

### Maven 依赖

```xml
<!-- Flink -->
<dependency>
    <groupId>org.apache.hudi</groupId>
    <artifactId>hudi-flink-bundle_2.12</artifactId>
    <version>0.14.1</version>
</dependency>

<!-- Spark -->
<dependency>
    <groupId>org.apache.hudi</groupId>
    <artifactId>hudi-spark3-bundle_2.12</artifactId>
    <version>0.14.1</version>
</dependency>
```

---

## Flink 集成

### 基础用法

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class HudiFlinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建 Hudi 表 (Copy on Write)
        tableEnv.executeSql(
            "CREATE TABLE hudi_trips (" +
            "  trip_id BIGINT," +
            "  driver_id BIGINT," +
            "  passenger_id BIGINT," +
            "  start_time TIMESTAMP(3)," +
            "  end_time TIMESTAMP(3)," +
            "  start_lon DOUBLE," +
            "  start_lat DOUBLE," +
            "  end_lon DOUBLE," +
            "  end_lat DOUBLE," +
            "  fare_amount DOUBLE," +
            "  _hoodie_operation_time TIMESTAMP(3)" +
            ") PARTITIONED BY (driver_id) " +
            "WITH (" +
            "  'connector' = 'hudi'," +
            "  'path' = 'hdfs://namenode:8020/hudi/trips'," +
            "  'table.type' = 'MERGE_ON_READ'," +
            "  'read.streaming.enabled' = 'true'," +
            "  'read.streaming.check-interval' = '60'" +
            ")"
        );

        // 2. 插入数据
        tableEnv.executeSql(
            "INSERT INTO hudi_trips " +
            "VALUES " +
            "(1, 1001, 2001, '2024-01-10 10:00:00', '2024-01-10 10:30:00', " +
            " 116.397, 39.916, 116.407, 39.926, 25.0)," +
            "(2, 1002, 2002, '2024-01-10 11:00:00', '2024-01-10 11:45:00', " +
            " 121.473, 31.230, 121.483, 31.240, 35.0)"
        );

        // 3. 查询数据
        Table result = tableEnv.sqlQuery(
            "SELECT trip_id, driver_id, fare_amount " +
            "FROM hudi_trips " +
            "WHERE fare_amount > 20"
        );
    }
}
```

### 流式写入

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class HudiStreamWriteExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(60000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建 Kafka 源表
        tableEnv.executeSql(
            "CREATE TABLE kafka_source (" +
            "  trip_id BIGINT," +
            "  driver_id BIGINT," +
            "  passenger_id BIGINT," +
            "  start_time TIMESTAMP(3)," +
            "  end_time TIMESTAMP(3)," +
            "  fare_amount DOUBLE" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'trips'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'properties.group.id' = 'testGroup'," +
            "  'scan.startup.mode' = 'latest-offset'," +
            "  'format' = 'json'" +
            ")"
        );

        // 2. 创建 Hudi 目标表
        tableEnv.executeSql(
            "CREATE TABLE hudi_sink (" +
            "  trip_id BIGINT," +
            "  driver_id BIGINT," +
            "  passenger_id BIGINT," +
            "  start_time TIMESTAMP(3)," +
            "  end_time TIMESTAMP(3)," +
            "  fare_amount DOUBLE" +
            ") WITH (" +
            "  'connector' = 'hudi'," +
            "  'path' = 'hdfs://namenode:8020/hudi/sink'," +
            "  'table.type' = 'MERGE_ON_READ'" +
            ")"
        );

        // 3. 数据写入
        tableEnv.executeSql(
            "INSERT INTO hudi_sink " +
            "SELECT * FROM kafka_source"
        );
    }
}
```

---

## Spark 集成

### 基础用法

```scala
import org.apache.spark.sql.SaveMode
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig

// 写入 Hudi
df.write
  .format("hudi")
  .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
  .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id")
  .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "date")
  .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
  .mode(SaveMode.Overwrite)
  .save("/path/to/hudi/table")

// 读取 Hudi
val df = spark.read.format("hudi").load("/path/to/hudi/table")
```

### Upsert 操作

```scala
// 配置
val hudiOptions = Map(
    "hoodie.table.name" -> "table",
    "hoodie.datasource.write.recordkey.field" -> "id",
    "hoodie.datasource.write.partitionpath.field" -> "date",
    "hoodie.datasource.write.precombine.field" -> "timestamp",
    "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.SimpleKeyGenerator"
)

// Upsert
df.write
  .format("hudi")
  .options(hudiOptions)
  .option("hoodie.insert.shuffle.parallelism", 100)
  .option("hoodie.upsert.shuffle.parallelism", 100)
  .mode(SaveMode.Append)
  .save("/path/to/hudi/table")
```

---

## 增量处理

### 增量查询

```scala
// 配置增量查询
val df = spark.read.format("hudi")
  .option("hoodie.datasource.query.type", "incremental")
  .option("hoodie.datasource.query.begin.instant.time", "20240110120000")
  .load("/path/to/hudi/table")

// 增量写入
df.write
  .format("hudi")
  .option("hoodie.datasource.write.operation", "insert")
  .options(hudiOptions)
  .mode(SaveMode.Append)
  .save("/path/to/hudi/table")
```

### Change Data Capture

```scala
// 提取变更数据
val changes = spark.read.format("hudi")
  .option("hoodie.datasource.query.type", "incremental")
  .option("hoodie.datasource.query.begin.instant.time", lastCommit)
  .load("/path/to/hudi/table")

// 处理变更
val inserts = changes.filter(col("_hoodie_is_insert"))
val updates = changes.filter(col("_hoodie_is_update"))
```

---

## 最佳实践

### 表类型选择

```scala
// 读多写少 -> COW
df.write
  .format("hudi")
  .option("hoodie.table.type", "COPY_ON_WRITE")
  .save("/path/to/hudi/table")

// 写多读少 -> MOR
df.write
  .format("hudi")
  .option("hoodie.table.type", "MERGE_ON_READ")
  .save("/path/to/hudi/table")
```

### 性能调优

```scala
// 并行度设置
df.write
  .format("hudi")
  .option("hoodie.insert.shuffle.parallelism", 200)
  .option("hoodie.upsert.shuffle.parallelism", 200)
  .save("/path/to/hudi/table")

// 文件大小设置
df.write
  .format("hudi")
  .option("hoodie.parquet.compression.codec", "zstd")
  .option("hoodie.target.file.max.size", 1024 * 1024 * 128) // 128MB
  .save("/path/to/hudi/table")
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-flink-integration.md](03-flink-integration.md) | Flink 集成 |
| [04-spark-integration.md](04-spark-integration.md) | Spark 集成 |
| [README.md](README.md) | 索引文档 |
