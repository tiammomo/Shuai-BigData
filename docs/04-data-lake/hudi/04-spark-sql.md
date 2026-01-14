# Apache Hudi Spark SQL 指南

## 目录

- [环境配置](#环境配置)
- [表操作](#表操作)
- [流批一体](#流批一体)
- [查询优化](#查询优化)

---

## 环境配置

### Spark 配置

```scala
// spark-shell 或 Spark SQL
spark.sql("SET spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension")

// Catalog 配置
spark.sql("""
  CREATE CATALOG hudi_catalog USING hudi
  WITH (
    'path' = 'hdfs://namenode:9000/hudi/warehouse'
  )
""")

USE CATALOG hudi_catalog;
```

### Maven 依赖

```xml
<dependency>
    <groupId>org.apache.hudi</groupId>
    <artifactId>hudi-spark3.4-bundle_2.12</artifactId>
    <version>0.14.1</version>
</dependency>
```

---

## 表操作

### 创建表

```sql
-- 创建 Copy-on-Write 表
CREATE TABLE hudi_cow (
    id BIGINT,
    name STRING,
    age INT,
    amount DECIMAL(10, 2),
    ts TIMESTAMP,
    PRIMARY KEY (id)
)
USING hudi
OPTIONS (
    'primaryKey' = 'id',
    'type' = 'cow',
    'path' = 'hdfs://namenode:9000/hudi/warehouse/cow_table'
);

-- 创建 Merge-on-Read 表
CREATE TABLE hudi_mor (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    ts TIMESTAMP,
    PRIMARY KEY (id)
)
USING hudi
OPTIONS (
    'primaryKey' = 'id',
    'type' = 'mor',
    'path' = 'hdfs://namenode:9000/hudi/warehouse/mor_table'
);

-- 创建分区表
CREATE TABLE hudi_partitioned (
    id BIGINT,
    name STRING,
    amount DECIMAL(10, 2),
    date STRING,
    PRIMARY KEY (id)
)
USING hudi
PARTITIONED BY (date)
OPTIONS (
    'primaryKey' = 'id',
    'type' = 'cow',
    'path' = 'hdfs://namenode:9000/hudi/warehouse/partitioned'
);
```

### 数据操作

```sql
-- 插入数据
INSERT INTO hudi_cow VALUES (1, 'Alice', 25, 100.0, CURRENT_TIMESTAMP);
INSERT INTO hudi_cow VALUES (2, 'Bob', 30, 200.0, CURRENT_TIMESTAMP);

-- 批量插入
INSERT INTO hudi_cow
SELECT * FROM source_table;

-- 更新数据
UPDATE hudi_cow
SET amount = 150.0
WHERE id = 1;

-- 删除数据
DELETE FROM hudi_cow
WHERE id = 1;

-- 合并数据
MERGE INTO hudi_cow AS target
USING source_updates AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET name = source.name, amount = source.amount
WHEN NOT MATCHED THEN
    INSERT (id, name, amount, ts) VALUES (source.id, source.name, source.amount, source.ts);
```

---

## 流批一体

### 批量查询

```sql
-- 全量读取
SELECT * FROM hudi_cow;

-- 带过滤
SELECT * FROM hudi_cow
WHERE date = '2024-01-15';

-- 聚合查询
SELECT date, COUNT(*) AS cnt, SUM(amount) AS total
FROM hudi_cow
GROUP BY date;
```

### 流式查询

```sql
-- 增量查询 (只读取新数据)
SET hoodie.datasource.query.type=incremental;
SET hoodie.datasource.query.begin.instant.time=20240115120000;

SELECT * FROM hudi_cow;

-- 时间旅行查询
SELECT * FROM hudi_cow VERSION AS OF 123456789;
SELECT * FROM hudi_cow TIMESTAMP AS OF '2024-01-15 12:00:00';
```

### Streaming Reads

```scala
// Spark Structured Streaming 读取 Hudi
val spark = SparkSession.builder()
    .appName("Hudi Streaming Read")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .getOrCreate()

val df = spark.readStream
    .format("hudi")
    .load("hdfs://namenode:9000/hudi/warehouse/cow_table")

df.createOrReplaceTempView("hudi_stream")

spark.sql("""
    SELECT id, name, count
    FROM hudi_stream
""").writeStream
    .format("console")
    .start()
```

---

## 查询优化

### 索引优化

```sql
-- Bloom Index (默认)
ALTER TABLE hudi_cow
SET TBLPROPERTIES (
    'hoodie.index.type' = 'BLOOM',
    'hoodie.bloom.index.filter.rate' = '0.05'
);

-- Bucket Index
ALTER TABLE hudi_cow
SET TBLPROPERTIES (
    'hoodie.index.type' = 'BUCKET',
    'hoodie.bucket.index.num.buckets' = '200'
);
```

### Compaction

```sql
-- 手动 Compaction (MOR 表)
CALL hoodie.system.compact('table_name');

-- 自动 Compaction 调度
ALTER TABLE hudi_cow
SET TBLPROPERTIES (
    'hoodie.compact.inline' = 'true',
    'hoodie.compact.schedule.inline' = 'true',
    'hoodie.compact.inline.max.delta.commits' = '10'
);
```

### 清理策略

```sql
-- 设置保留时间
ALTER TABLE hudi_cow
SET TBLPROPERTIES (
    'hoodie.cleaner.commits.retained' = '30',
    'hoodie.cleaner.hours.retained' = '168'
);
```

---

## 常见操作

### 表管理

```sql
-- 查看表列表
SHOW TABLES;

-- 查看表信息
DESCRIBE EXTENDED hudi_cow;

-- 删除表
DROP TABLE IF EXISTS hudi_cow;

-- 修复表
CALL hoodie.system.run.cleanup('table_name');
```

### 归档

```sql
-- 清理旧版本
CALL hoodie.system.clean('table_name');

-- 归档配置
ALTER TABLE hudi_cow
SET TBLPROPERTIES (
    'hoodie.archive.auto' = 'true',
    'hoodie.archive.min.commits' = '20',
    'hoodie.archive.max.commits' = '30'
);
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作 |
| [03-flink-integration.md](03-flink-integration.md) | Flink 集成 |
| [README.md](README.md) | 索引文档 |
