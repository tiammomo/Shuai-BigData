# Apache Iceberg Flink 集成指南

## 目录

- [环境配置](#环境配置)
- [Flink SQL](#flink-sql)
- [流式读写](#流式读写)
- [Flink DataStream](#flink-datastream)

---

## 环境配置

### Flink SQL Client

```sql
-- 创建 Hadoop Catalog
CREATE CATALOG hadoop_iceberg WITH (
    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog',
    'warehouse' = 'hdfs://namenode:9000/iceberg/warehouse'
);

-- 创建 Hive Catalog
CREATE CATALOG hive_iceberg WITH (
    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.hive.HiveCatalog',
    'uri' = 'thrift://hive-metastore:9083'
);

USE CATALOG hadoop_iceberg;
```

### Maven 依赖

```xml
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-flink-runtime-3.4_2.12</artifactId>
    <version>1.5.2</version>
</dependency>
```

---

## Flink SQL

### 创建表

```sql
-- 创建 Iceberg 表
CREATE TABLE iceberg_table (
    id BIGINT,
    name STRING,
    age INT,
    email STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet'
);

-- 创建分区表
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    created_at TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) PARTITIONED BY (days(created_at), status)
WITH (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728'
);

-- 创建无主键表
CREATE TABLE events (
    event_id BIGINT,
    event_time TIMESTAMP(3),
    event_type STRING,
    data STRING
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);
```

### 数据操作

```sql
-- 插入数据
INSERT INTO iceberg_table
VALUES (1, 'Alice', 25, 'alice@email.com');

-- 批量插入
INSERT INTO iceberg_table
SELECT * FROM source_table;

-- 覆盖写入
INSERT OVERWRITE iceberg_table
SELECT * FROM new_source;

-- 更新 (需要主键)
UPDATE iceberg_table
SET age = 26
WHERE id = 1;

-- 删除 (需要主键)
DELETE FROM iceberg_table
WHERE id = 1;
```

---

## 流式读写

### 流式写入

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// Kafka 源
tableEnv.executeSql(
    "CREATE TABLE kafka_source (" +
    "  id BIGINT," +
    "  name STRING," +
    "  amount DECIMAL(10, 2)," +
    "  ts TIMESTAMP(3)" +
    ") WITH (" +
    "  'connector' = 'kafka'," +
    "  'topic' = 'events'," +
    "  'properties.bootstrap.servers' = 'localhost:9092'," +
    "  'scan.startup.mode' = 'latest-offset'," +
    "  'format' = 'json'" +
    ")"
);

// Iceberg 目标
tableEnv.executeSql(
    "CREATE TABLE iceberg_sink (" +
    "  id BIGINT," +
    "  name STRING," +
    "  amount DECIMAL(10, 2)," +
    "  ts TIMESTAMP(3)," +
    "  PRIMARY KEY (id) NOT ENFORCED" +
    ") WITH (" +
    "  'connector' = 'iceberg'," +
    "  'catalog-name' = 'hadoop_iceberg'," +
    "  'catalog-database' = 'default'," +
    "  'table-name' = 'iceberg_sink'," +
    "  'format-version' = '2'" +
    ")"
);

// 写入
tableEnv.executeSql(
    "INSERT INTO iceberg_sink " +
    "SELECT id, name, amount, ts " +
    "FROM kafka_source"
);
```

### 流式读取

```sql
-- 追加流读
CREATE TABLE iceberg_stream AS
SELECT * FROM iceberg_table;

-- 带 watermark 的流读
CREATE TABLE events_stream AS
SELECT *
FROM events
WHERE event_time >= TIMESTAMP '2024-01-15 00:00:00';

-- 批量流读
CREATE TABLE iceberg_batch AS
SELECT *
FROM iceberg_table
WITH (streaming = 'false');
```

### 增量读取

```sql
-- 配置增量读取
CREATE TABLE iceberg_incremental AS
SELECT *
FROM iceberg_table
/*+ OPTIONS (
    'monitor-interval' = '10s',
    'start-tag' = 'v1',
    'end-tag' = 'v2'
) */;
```

---

## Flink DataStream

### 写入 Iceberg

```java
import org.apache.iceberg.flink.writer.DataWriter;
import org.apache.iceberg.flink.writer.FlinkAppender;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);

// 创建 DataStream
DataStream<Record> recordStream = env
    .addSource(new MySource())
    .map(MyRecord::toIcebergRecord);

// 写入 Iceberg
DataStream<Record> result = recordStream
    .forceNonParallel()
    .map(new IcebergSinkFunction<>(
        table,
        schema,
        partitionSpec,
        appenderFactory,
        fileAppenderFactory,
        committer
    ));

private static class MySource extends RichSourceFunction<MyRecord> {
    @Override
    public void run(SourceContext<MyRecord> ctx) throws Exception {
        // 生成数据
    }
}
```

### 读取 Iceberg

```java
// 使用 FlinkSource
FlinkSource.Builder<Record> builder = FlinkSource.forRecord(icebergTable);

DataStream<Record> recordStream = builder
    .project(schema)
    .split(startingTag, endingTag)
    .build();

// 处理
recordStream
    .map(record -> ...)
    .addSink(new MySink());
```

---

## 最佳实践

### 写入配置

```sql
-- 优化文件大小
CREATE TABLE optimized_table (
    id BIGINT,
    data STRING
) WITH (
    'write.target-file-size-bytes' = '268435456',  -- 256MB
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true'
);

-- 启用 Upsert
CREATE TABLE upsert_table (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'write.upsert.enabled' = 'true',
    'write.upsert.mode' = 'merge-on-read'
);
```

### 并行度配置

```java
// 调整写入并行度
tableEnv.getConfig().set("write.parquet.target-file-size", "134217728");

// 调整 IOManager
env.getConfig().enableSysoutLogging();
```

### Checkpoint 配置

```java
// 启用 Checkpoint
env.enableCheckpointing(60000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// 配置 State Backend
env.setStateBackend(new RocksDBStateBackend("hdfs://namenode:9000/checkpoints", true));
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作 |
| [04-spark-sql.md](04-spark-sql.md) | Spark SQL 指南 |
| [README.md](README.md) | 索引文档 |
