# Delta Lake Flink SQL 指南

## 目录

- [环境配置](#环境配置)
- [表操作](#表操作)
- [流式读写](#流式读写)
- [变更数据捕获](#变更数据捕获)

---

## 环境配置

### Flink SQL Client

```sql
-- 创建 Delta Lake Catalog
CREATE CATALOG delta_catalog WITH (
    'type' = 'delta',
    'path' = 'hdfs://namenode:9000/delta-warehouse',
    'delta.enable-change-data-feed' = 'true'
);

USE CATALOG delta_catalog;
```

### Maven 依赖

```xml
<dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-flink-connector</artifactId>
    <version>3.0.0</version>
</dependency>
```

---

## 表操作

### 创建表

```sql
-- 创建 Delta 表 (Flink SQL)
CREATE TABLE delta_table (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    status STRING,
    event_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'delta',
    'path' = 'hdfs://namenode:9000/delta-warehouse/db/table',
    'delta.create-branch' = 'main'
);

-- 创建分区表
CREATE TABLE delta_partitioned (
    id BIGINT,
    data STRING,
    date STRING,
    PRIMARY KEY (id) NOT ENFORCED
) PARTITIONED BY (date)
WITH (
    'connector' = 'delta',
    'path' = 'hdfs://namenode:9000/delta-warehouse/db/partitioned'
);
```

### 读取表

```sql
-- 批量读取
SELECT * FROM delta_table WHERE status = 'active';

-- 聚合查询
SELECT name, SUM(amount) AS total
FROM delta_table
GROUP BY name;
```

### 写入表

```sql
-- 追加写入
INSERT INTO delta_table
VALUES (1, 'Alice', 100.0, 'active', CURRENT_TIMESTAMP);

-- 覆盖写入 (静态)
INSERT OVERWRITE delta_table
VALUES (1, 'Alice', 100.0, 'active', CURRENT_TIMESTAMP);

-- 从查询写入
INSERT INTO delta_table
SELECT * FROM source_table;
```

---

## 流式读写

### 流式写入

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 从 Kafka 读取
tableEnv.executeSql(
    "CREATE TABLE kafka_source (" +
    "  id BIGINT," +
    "  name STRING," +
    "  amount DECIMAL(10, 2)," +
    "  event_time TIMESTAMP(3)" +
    ") WITH (" +
    "  'connector' = 'kafka'," +
    "  'topic' = 'events'," +
    "  'properties.bootstrap.servers' = 'localhost:9092'," +
    "  'format' = 'json'" +
    ")"
);

// 写入 Delta Lake
tableEnv.executeSql(
    "CREATE TABLE delta_sink (" +
    "  id BIGINT," +
    "  name STRING," +
    "  amount DECIMAL(10, 2)," +
    "  event_time TIMESTAMP(3)," +
    "  PRIMARY KEY (id) NOT ENFORCED" +
    ") WITH (" +
    "  'connector' = 'delta'," +
    "  'path' = 'hdfs://namenode:9000/delta-warehouse/sink'" +
    ")"
);

// 插入数据
tableEnv.executeSql("INSERT INTO delta_sink SELECT * FROM kafka_source");
```

### 流式读取

```sql
-- 追加模式流读
CREATE TABLE delta_stream AS
SELECT * FROM delta_table;

-- 带 watermark 的流读
CREATE TABLE delta_streaming AS
SELECT *
FROM delta_table
LIKE delta_table;

-- 时间旅行读取
CREATE TABLE delta_timetravel AS
SELECT * FROM delta_table VERSION AS OF 123456789;
```

---

## 变更数据捕获

### CDF (Change Data Feed)

```sql
-- 启用 CDF
ALTER TABLE delta_table
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- 读取变更数据
CREATE TABLE cdc_table (
    _change_type STRING,
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    _commit_version BIGINT,
    _commit_timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'delta',
    'path' = 'hdfs://namenode:9000/delta-warehouse/db/table',
    'delta.read.change-data-feed' = 'true'
);

-- 查询变更
SELECT * FROM cdc_table
WHERE _change_type IN ('insert', 'update_after', 'delete');
```

### 增量聚合

```sql
-- 增量物化视图
CREATE MATERIALIZED VIEW mv_count AS
SELECT status, COUNT(*) AS cnt
FROM delta_table
GROUP BY status;

-- 增量消费
SELECT * FROM mv_count;
```

---

## 最佳实践

### 分区裁剪

```sql
-- 按分区查询
SELECT * FROM delta_partitioned
WHERE date = '2024-01-15';

-- 避免全表扫描
SELECT id, amount
FROM delta_table
WHERE date >= '2024-01-01' AND date < '2024-02-01';
```

### Z-Order 优化

```sql
-- 优化文件布局
OPTIMIZE delta_table
ZORDER BY (id, date);

-- 真空清理
VACUUM delta_table RETAIN 168 HOURS;
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作 |
| [03-spark-integration.md](03-spark-integration.md) | Spark 集成 |
| [README.md](README.md) | 索引文档 |
