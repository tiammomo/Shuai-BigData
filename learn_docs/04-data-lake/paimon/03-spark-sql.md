# Apache Paimon Spark SQL 指南

## 目录

- [环境配置](#环境配置)
- [表操作](#表操作)
- [流批一体](#流批一体)
- [性能优化](#性能优化)

---

## 环境配置

### Spark 配置

```scala
spark.sql("SET spark.sql.catalog.paimon = org.apache.paimon.spark.SparkCatalog")
spark.sql("SET spark.sql.catalog.paimon.type = hive")
spark.sql("SET spark.sql.catalog.paimon.uri = thrift://hive-metastore:9083")
spark.sql("SET spark.sql.catalog.paimon.warehouse = hdfs://namenode:9000/paimon/warehouse")

USE CATALOG paimon;
```

### Maven 依赖

```xml
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-spark-3.4_2.12</artifactId>
    <version>1.1.0</version>
</dependency>
```

---

## 表操作

### 创建表

```sql
-- 创建主键表
CREATE TABLE paimon_keyed (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    status STRING,
    PRIMARY KEY (id)
) WITH (
    'bucket' = '4',
    'bucket-key' = 'id',
    'changelog-producer' = 'input',
    'merge-engine' = 'deduplicate'
);

-- 创建 Append 表
CREATE TABLE paimon_append (
    id BIGINT,
    data STRING,
    event_time TIMESTAMP(3)
) WITH (
    'merge-engine' = 'first-row',
    'compaction.min.file-num' = '5'
);

-- 创建分区表
CREATE TABLE paimon_partitioned (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    date STRING,
    PRIMARY KEY (id)
) PARTITIONED BY (date)
WITH (
    'bucket' = '8',
    'merge-engine' = 'aggregation'
);

-- 创建 Tag 表
CREATE TABLE tagged_table (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2)
) WITH (
    'bucket' = '4'
);

-- 创建 Tag
CALL paimon.system.create_tag('tagged_table', 'v1', 123456789);
```

### 数据操作

```sql
-- 插入数据
INSERT INTO paimon_keyed
VALUES (1, 'Alice', 100.0, 'active');

INSERT INTO paimon_keyed
VALUES (2, 'Bob', 200.0, 'pending');

-- 批量插入
INSERT INTO paimon_keyed
SELECT * FROM source_table;

-- 覆盖写入
INSERT OVERWRITE paimon_keyed
SELECT * FROM new_source;

-- 更新数据
UPDATE paimon_keyed
SET amount = 150.0
WHERE id = 1;

-- 删除数据
DELETE FROM paimon_keyed
WHERE id = 1;

-- 合并数据
MERGE INTO paimon_keyed AS target
USING source_updates AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET name = source.name, amount = source.amount
WHEN NOT MATCHED THEN
    INSERT (id, name, amount, status) VALUES (source.id, source.name, source.amount, source.status);
```

---

## 流批一体

### 批量查询

```sql
-- 全量读取
SELECT * FROM paimon_keyed;

-- 过滤查询
SELECT * FROM paimon_keyed
WHERE status = 'active';

-- 聚合查询
SELECT status, COUNT(*) AS cnt, SUM(amount) AS total
FROM paimon_keyed
GROUP BY status;

-- 按分区查询
SELECT * FROM paimon_partitioned
WHERE date = '2024-01-15';
```

### 时间旅行

```sql
-- 查看快照
SELECT * FROM paimon_keyed$snapshots;

-- 按版本查询
SELECT * FROM paimon_keyed VERSION AS OF 123456789;

-- 按时间查询
SELECT * FROM paimon_keyed TIMESTAMP AS OF '2024-01-15 12:00:00';
```

### 增量查询

```sql
-- 配置增量读取
SET paimon.scan.mode = 'incremental';
SET paimon.scan.start-tag = 'v1';
SET paimon.scan.end-tag = 'v2';

SELECT * FROM paimon_keyed;

-- 重置
RESET paimon.scan.mode;
```

### Spark Structured Streaming

```scala
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame

// 流式读取
val streamDF = spark.readStream
    .format("paimon")
    .option("path", "hdfs://namenode:9000/paimon/warehouse/paimon_keyed")
    .load()

// 流式写入
streamDF.writeStream
    .format("paimon")
    .option("path", "hdfs://namenode:9000/paimon/warehouse/output")
    .option("checkpointLocation", "/tmp/checkpoint")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .start()
```

---

## 性能优化

### Bucket 优化

```sql
-- 调整分桶数
CREATE TABLE optimized_buckets (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2)
) WITH (
    'bucket' = '16',  -- 调整分桶数
    'bucket-key' = 'id'
);

-- 选择合适的 bucket-key
-- 原则: 高基数、频繁查询、用于 JOIN
CREATE TABLE good_buckets (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    date STRING
) WITH (
    'bucket' = '32',
    'bucket-key' = 'order_id, user_id'  -- 多列分桶
);
```

### Compaction

```sql
-- 手动 Compaction
CALL paimon.system.compact('table_name');

-- 指定分桶 Compaction
CALL paimon.system.compact(
    'table_name',
    'bucket = 1'  -- 只 Compaction bucket 1
);

-- 大 Compaction
CALL paimon.system.compact(
    'table_name',
    'strategy' = 'full'
);
```

### 过期清理

```sql
-- 设置 TTL
ALTER TABLE paimon_keyed
SET TBLPROPERTIES (
    'table.exec.state.ttl' = '7d'
);

-- 清理旧快照
CALL paimon.system.expire_snapshots(
    'table_name',
    30,  -- 保留30天
    100   -- 最少保留100个快照
);

-- 清理未引用文件
CALL paimon.system.clean_new_files(
    'table_name'
);
```

### 向量化读取

```sql
-- 启用向量化读取
ALTER TABLE paimon_keyed
SET TBLPROPERTIES (
    'read.vectorized.enabled' = 'true',
    'read.batch-size' = '4096'
);
```

---

## 最佳实践

### 表设计

```sql
-- 推荐: 主键 + 分区 + 分桶
CREATE TABLE best_practice (
    order_id BIGINT,
    user_id BIGINT,
    product_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    created_at TIMESTAMP(3),
    date STRING,
    PRIMARY KEY (order_id, date)  -- 复合主键
) PARTITIONED BY (date)
WITH (
    'bucket' = '32',
    'bucket-key' = 'order_id',
    'merge-engine' = 'aggregation'
);

-- 按时间分区设计
CREATE TABLE time_series (
    id BIGINT,
    metric_name STRING,
    metric_value DOUBLE,
    timestamp TIMESTAMP(3),
    dt STRING
) PARTITIONED BY (dt)
WITH (
    'bucket' = '16',
    'bucket-key' = 'id',
    'merge-engine' = 'aggregation'
);
```

### 查询优化

```sql
-- 1. 使用分区裁剪
SELECT * FROM paimon_partitioned
WHERE date = '2024-01-15';  -- 最佳实践

-- 避免
SELECT * FROM paimon_partitioned
WHERE DATE(created_at) = '2024-01-15';  -- 无法裁剪

-- 2. 使用 bucket-key 过滤
SELECT * FROM paimon_keyed
WHERE id = 12345;  -- 定位到特定分桶

-- 3. 避免 SELECT *
SELECT id, name FROM paimon_keyed;  -- 推荐
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-usage.md](02-usage.md) | 使用指南 |
| [README.md](README.md) | 索引文档 |
