# Apache Iceberg Spark SQL 指南

## 目录

- [环境配置](#环境配置)
- [表操作](#表操作)
- [时间旅行](#时间旅行)
- [高级特性](#高级特性)

---

## 环境配置

### Spark 配置

```scala
spark.sql("SET spark.sql.catalog.spark_catalog = org.apache.iceberg.spark.SparkSessionCatalog")
spark.sql("SET spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtension")

// Hadoop Catalog
spark.sql("""
  CREATE CATALOG hadoop_catalog USING iceberg
  WITH ('type' = 'iceberg', 'warehouse' = 'hdfs://namenode:9000/iceberg/warehouse')
""")

// Hive Catalog
spark.sql("""
  CREATE CATALOG hive_catalog USING iceberg
  WITH ('type' = 'iceberg', 'catalog-impl' = 'org.apache.iceberg.hive.HiveCatalog', 'uri' = 'thrift://hive:9083')
""")
```

### Maven 依赖

```xml
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-spark-runtime-3.4_2.12</artifactId>
    <version>1.5.2</version>
</dependency>
```

---

## 表操作

### 创建表

```sql
-- 使用 CTAS 创建
CREATE TABLE spark_catalog.db.users (
    id BIGINT,
    name STRING,
    age INT,
    email STRING
) USING iceberg
PARTITIONED BY (age);

-- 使用 DDL 创建
CREATE TABLE spark_catalog.db.orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    created_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(created_at), status)
TBLPROPERTIES (
    'write.target-file-size-bytes' = '536870912',
    'write.format.default' = 'parquet'
);

-- 创建对齐表
CREATE TABLE aligned_table (
    id BIGINT,
    name STRING,
    value DOUBLE
) USING iceberg;
```

### 数据操作

```sql
-- 插入数据
INSERT INTO spark_catalog.db.users
VALUES (1, 'Alice', 25, 'alice@email.com'),
       (2, 'Bob', 30, 'bob@email.com');

-- 批量插入
INSERT INTO spark_catalog.db.users
SELECT * FROM source_table;

-- 覆盖写入
INSERT OVERWRITE spark_catalog.db.users
SELECT * FROM new_source;

-- 更新数据
UPDATE spark_catalog.db.users
SET age = 26
WHERE id = 1;

-- 删除数据
DELETE FROM spark_catalog.db.users
WHERE id = 1;

-- 合并数据
MERGE INTO spark_catalog.db.users AS target
USING source_users AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET name = source.name, age = source.age
WHEN NOT MATCHED THEN
    INSERT (id, name, age, email)
    VALUES (source.id, source.name, source.age, source.email);
```

---

## 时间旅行

### 历史版本查询

```sql
-- 查看历史快照
SELECT * FROM spark_catalog.db.users HISTORY;

-- 按版本查询
SELECT * FROM spark_catalog.db.users
VERSION AS OF 12345678901234;

-- 按时间查询
SELECT * FROM spark_catalog.db.users
TIMESTAMP AS OF '2024-01-15 12:00:00';

-- 增量查询 (新版本)
SELECT * FROM spark_catalog.db.users
SINCE VERSION AS OF 12345678901234;
```

### 分支操作

```sql
-- 创建分支
ALTER TABLE spark_catalog.db.users
CREATE BRANCH branch_2024 AS OF VERSION 12345678901234;

-- 在分支上操作
INSERT INTO spark_catalog.db.users
SELECT * FROM temp_data
WHERE _spec_id = 0 AND _partition = 'bucket=1';

-- 查询分支
SELECT * FROM spark_catalog.db.users
VERSION AS OF 'branch_2024';

-- 删除分支
ALTER TABLE spark_catalog.db.users
DROP BRANCH branch_2024;
```

### 标签操作

```sql
-- 创建标签
ALTER TABLE spark_catalog.db.users
CREATE TAG v1.0 AS OF VERSION 12345678901234;

-- 使用标签
SELECT * FROM spark_catalog.db.users
VERSION AS OF 'v1.0';

-- 删除标签
ALTER TABLE spark_catalog.db.users
DROP TAG v1.0;
```

---

## 高级特性

### Schema 演进

```sql
-- 添加列
ALTER TABLE spark_catalog.db.users
ADD COLUMNS (
    phone STRING COMMENT 'phone number',
    address STRING AFTER email
);

-- 删除列
ALTER TABLE spark_catalog.db.users
DROP COLUMN phone;

-- 重命名列
ALTER TABLE spark_catalog.db.users
RENAME COLUMN email TO contact;

-- 修改列类型
ALTER TABLE spark_catalog.db.users
ALTER COLUMN age TYPE BIGINT;

-- 修改列注释
ALTER TABLE spark_catalog.db.users
ALTER COLUMN age COMMENT 'user age';
```

### 分区演进

```sql
-- 改变分区变换
ALTER TABLE spark_catalog.db.orders
SET PARTITION SPEC (days(created_at), bucket(16, user_id));

-- 重置分区
ALTER TABLE spark_catalog.db.orders
SET PARTITION SPEC (created_at);

-- 添加新分区列
ALTER TABLE spark_catalog.db.orders
SET PARTITION SPEC (days(created_at), status);
```

### 物化视图

```sql
-- 创建物化视图
CREATE MATERIALIZED VIEW daily_stats AS
SELECT
    DATE(created_at) AS stat_date,
    status,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM spark_catalog.db.orders
GROUP BY DATE(created_at), status;

-- 刷新物化视图
REFRESH MATERIALIZED VIEW daily_stats;

-- 增量刷新
REFRESH MATERIALIZED VIEW daily_stats
AS SELECT
    DATE(created_at) AS stat_date,
    status,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM spark_catalog.db.orders
WHERE created_at > LAST_REFRESH_TIME
GROUP BY DATE(created_at), status;
```

### 元数据表

```sql
-- 查看快照
SELECT * FROM spark_catalog.db.users$snapshots;

-- 查看清单列表
SELECT * FROM spark_catalog.db.users$manifests;

-- 查看数据文件
SELECT * FROM spark_catalog.db.users$files;

-- 查看分区
SELECT * FROM spark_catalog.db.users$partitions;
```

---

## 性能优化

### Compaction

```sql
-- 手动执行 Compaction
CALL spark_catalog.system.rewrite_data_files(
    table => 'spark_catalog.db.users',
    strategy => 'sort',
    sortOrder => 'name ASC, age DESC'
);

-- 增量 Compaction
CALL spark_catalog.system.rewrite_position_delete_files(
    table => 'spark_catalog.db.users'
);

-- 优化文件布局
OPTIMIZE spark_catalog.db.users
ZORDER BY (age, name);
```

### 清理

```sql
-- 清理过期快照
CALL spark_catalog.system.expire_snapshots(
    table => 'spark_catalog.db.users',
    older_than => SYSTEM_TIMESTAMP() - INTERVAL 30 DAYS,
    retain_last => 5
);

-- 移除孤立文件
CALL spark_catalog.system.remove_orphan_files(
    table => 'spark_catalog.db.users',
    older_than => SYSTEM_TIMESTAMP() - INTERVAL 7 DAYS
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
