# Apache Hudi Flink 集成指南

## 目录

- [环境配置](#环境配置)
- [Flink SQL](#flink-sql)
- [Flink DataStream](#flink-datastream)
- [流式同步](#流式同步)

---

## 环境配置

### Flink SQL Client

```sql
-- 创建 Hudi Catalog
CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'path' = 'hdfs://namenode:9000/hudi/warehouse',
    'default-database' = 'default'
);

USE CATALOG hudi_catalog;
```

### Maven 依赖

```xml
<dependency>
    <groupId>org.apache.hudi</groupId>
    <artifactId>hudi-flink-bundle_2.12</artifactId>
    <version>0.14.1</version>
</dependency>
```

---

## Flink SQL

### 创建表

```sql
-- 创建 Copy-on-Write 表
CREATE TABLE flink_cow (
    id BIGINT,
    name STRING,
    age INT,
    amount DECIMAL(10, 2),
    ts TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://namenode:9000/hudi/warehouse/cow_table',
    'table.type' = 'cow',
    'write.bucket.assign.mode' = 'fixed'
);

-- 创建 Merge-on-Read 表
CREATE TABLE flink_mor (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    ts TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://namenode:9000/hudi/warehouse/mor_table',
    'table.type' = 'mor',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '60'
);

-- 创建分区表
CREATE TABLE flink_partitioned (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    dt STRING,
    hr STRING,
    PRIMARY KEY (id) NOT ENFORCED
) PARTITIONED BY (dt, hr)
WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://namenode:9000/hudi/warehouse/partitioned',
    'table.type' = 'cow'
);
```

### 数据操作

```sql
-- 插入数据
INSERT INTO flink_cow
VALUES (1, 'Alice', 25, 100.0, CURRENT_TIMESTAMP);

-- 批量插入
INSERT INTO flink_cow
SELECT * FROM source_table;

-- 更新数据
UPDATE flink_cow
SET amount = 150.0
WHERE id = 1;

-- 删除数据
DELETE FROM flink_cow
WHERE id = 1;
```

### 流式读取

```sql
-- 启用流式读取
CREATE TABLE flink_streaming (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    ts TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://namenode:9000/hudi/warehouse/table',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '60',
    'read.streaming.start-commit' = '20240115120000'
);

-- 从 Flink 读取流
SELECT * FROM flink_streaming;
```

---

## Flink DataStream

### 写入 Hudi

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.sink.common.WriteFunction;
import org.apache.hudi.sink.utils.WriteConcurrencyControl;
import org.apache.hudi.table.HoodieTable;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);
env.enableCheckpointing(60000);

StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 使用 Table API
tableEnv.executeSql(
    "CREATE TABLE source_table (" +
    "  id BIGINT," +
    "  name STRING," +
    "  amount DECIMAL(10, 2)," +
    "  ts TIMESTAMP(3)" +
    ") WITH (" +
    "  'connector' = 'kafka'," +
    "  'topic' = 'source'," +
    "  'properties.bootstrap.servers' = 'localhost:9092'," +
    "  'format' = 'json'" +
    ")"
);

tableEnv.executeSql(
    "CREATE TABLE hudi_sink (" +
    "  id BIGINT," +
    "  name STRING," +
    "  amount DECIMAL(10, 2)," +
    "  ts TIMESTAMP(3)," +
    "  PRIMARY KEY (id) NOT ENFORCED" +
    ") WITH (" +
    "  'connector' = 'hudi'," +
    "  'path' = 'hdfs://namenode:9000/hudi/warehouse/sink'," +
    "  'table.type' = 'cow'" +
    ")"
);

tableEnv.executeSql("INSERT INTO hudi_sink SELECT * FROM source_table");
```

### 自定义 Write Function

```java
// 使用 OutputFormat
DataStream<HoodieRecord> input = ...

HudiSinkFunction<HoodieRecord> sinkFunction = new HudiSinkFunction<>(
    new HoodieWriteConfig.Builder()
        .withBasePath("hdfs://namenode:9000/hudi/warehouse/table")
        .withSchema("...")
        .build(),
    new HadoopConfigurations()
);

input.addSink(sinkFunction);
```

---

## 流式同步

### Kafka → Hudi

```java
// 从 Kafka 同步到 Hudi
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// Kafka 源
tableEnv.executeSql(
    "CREATE TABLE kafka_source (" +
    "  id BIGINT," +
    "  name STRING," +
    "  amount DECIMAL(10, 2)," +
    "  event_time TIMESTAMP(3)," +
    "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND" +
    ") WITH (" +
    "  'connector' = 'kafka'," +
    "  'topic' = 'orders'," +
    "  'properties.bootstrap.servers' = 'localhost:9092'," +
    "  'scan.startup.mode' = 'latest-offset'," +
    "  'format' = 'json'" +
    ")"
);

// Hudi 目标
tableEnv.executeSql(
    "CREATE TABLE hudi_orders (" +
    "  id BIGINT," +
    "  name STRING," +
    "  amount DECIMAL(10, 2)," +
    "  event_time TIMESTAMP(3)," +
    "  PRIMARY KEY (id) NOT ENFORCED" +
    ") WITH (" +
    "  'connector' = 'hudi'," +
    "  'path' = 'hdfs://namenode:9000/hudi/warehouse/orders'," +
    "  'table.type' = 'mor'," +
    "  'write.bucket.assign.mode' = 'fixed'," +
    "  'write.precombine.field' = 'event_time'" +
    ")"
);

// 同步
tableEnv.executeSql(
    "INSERT INTO hudi_orders " +
    "SELECT id, name, amount, event_time " +
    "FROM kafka_source"
);
```

### MySQL CDC → Hudi

```sql
-- Flink SQL CDC 同步
CREATE TABLE mysql_source (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = 'password',
    'database-name' = 'mydb',
    'table-name' = 'orders'
);

CREATE TABLE hudi_sink (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://namenode:9000/hudi/warehouse/orders',
    'table.type' = 'mor'
);

INSERT INTO hudi_sink
SELECT * FROM mysql_source;
```

---

## 最佳实践

### 写入配置

```sql
-- 批量写入配置
CREATE TABLE hudi_tuned (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    ts TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://namenode:9000/hudi/warehouse/tuned',
    'write.bucket.assign.mode' = 'fixed',
    'write.parquet.file.size' = '134217728',  -- 128MB
    'write.merge.max.size' = '1073741824',    -- 1GB
    'compaction.async.enabled' = 'true',
    'compaction.schedule.enabled' = 'true',
    'compaction.trigger.strategy' = 'num_commits',
    'compaction.target.file.size' = '1073741824'
);
```

### 性能优化

```sql
-- 调整并发
SET 'parallelism.default' = '8';

-- 内存配置
SET 'taskmanager.memory.process.size' = '4096m';

-- Checkpoint 配置
SET 'execution.checkpointing.interval' = '5min';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作 |
| [04-spark-sql.md](04-spark-sql.md) | Spark SQL 指南 |
| [README.md](README.md) | 索引文档 |
