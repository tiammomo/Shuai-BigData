# Apache Paimon 使用指南

## 目录

- [Flink 集成](#flink-集成)
- [Spark 集成](#spark-集成)
- [CDC 同步](#cdc-同步)
- [查询优化](#查询优化)

---

## Flink 集成

### 环境配置

```sql
-- 创建 Catalog
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs://namenode:9000/paimon/warehouse',
    'metastore' = 'hive',
    'uri' = 'thrift://hive-metastore:9083'
);

USE CATALOG paimon_catalog;
```

### 创建表

```sql
-- 主键表 (CDC 场景)
CREATE TABLE users (
    user_id BIGINT,
    name STRING,
    email STRING,
    age INT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'bucket-key' = 'user_id',
    'changelog-producer' = 'input',
    'merge-engine' = 'deduplicate'
);

-- Append 表 (日志场景)
CREATE TABLE events (
    event_id BIGINT,
    event_time TIMESTAMP(3),
    event_type STRING,
    data STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'merge-engine' = 'first-row',
    'compaction.min.file-num' = '5'
);

-- 分区表
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (order_id, create_date) NOT ENFORCED
) PARTITIONED BY (create_date)
WITH (
    'bucket' = '8',
    'merge-engine' = 'aggregation'
);
```

### 数据操作

```sql
-- 插入数据
INSERT INTO users VALUES (1, 'Alice', 'alice@email.com', 25);
INSERT INTO users VALUES (2, 'Bob', 'bob@email.com', 30);

-- 批量插入
INSERT INTO users SELECT * FROM source_users;

-- 更新数据 (主键表)
UPDATE users SET age = 26 WHERE user_id = 1;

-- 删除数据 (主键表)
DELETE FROM users WHERE user_id = 1;
```

### 流式写入

```java
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.datagen.table.DataGenConnectorSourceFunction;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);

// 创建 Paimon 表
catalog.createTable("my_db.my_table", schema);

// 使用 FlinkSink
env.fromSource(new DataGenConnectorSourceFunction(), WatermarkStrategy.noWatermarks(), "source")
    .sinkTo(FlinkSinkBuilder.forRow(
        catalog.table("my_db.my_table"),
        FlinkRowDataSerializer::new
    ).build());

env.execute("Paimon Stream Write");
```

---

## Spark 集成

### Spark SQL

```sql
-- 读取 Paimon 表
SELECT * FROM paimon_catalog.my_db.users;

-- 批量写入
INSERT INTO TABLE paimon_catalog.my_db.users
SELECT * FROM source_table;

-- 时间旅行查询
SELECT * FROM users VERSION AS OF 123456789;
SELECT * FROM users TIMESTAMP AS OF '2024-01-15 12:00:00';
```

### Spark API

```scala
import org.apache.paimon.spark._

// 读取
val df = spark.read.format("paimon")
    .option("path", "hdfs://namenode:9000/paimon/warehouse/my_db/users")
    .load()

// 写入
df.write.format("paimon")
    .option("path", "hdfs://namenode:9000/paimon/warehouse/my_db/target")
    .mode("append")
    .save()
```

---

## CDC 同步

### MySQL CDC → Paimon

```sql
-- 创建 MySQL CDC 表
CREATE TABLE mysql_orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = 'password',
    'database-name' = 'orders_db',
    'table-name' = 'orders'
);

-- 同步到 Paimon
CREATE TABLE paimon_orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'warehouse' = 'hdfs://namenode:9000/paimon/warehouse'
);

INSERT INTO paimon_orders SELECT * FROM mysql_orders;
```

### 整库同步

```sql
-- 使用 Flink CDC 整库同步
CREATE TABLE mysql_db (
    `.*` STRING
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'database-name' = 'source_db',
    'table-name' = '.*',
    'username' = 'root',
    'password' = 'password'
);

-- 写入 Paimon
INSERT INTO paimon_db
SELECT * FROM mysql_db;
```

---

## 查询优化

### 分区裁剪

```sql
-- 按分区查询
SELECT * FROM orders
WHERE create_date = '2024-01-15';  -- 自动裁剪分区

-- 多分区
SELECT * FROM orders
WHERE create_date >= '2024-01-15' AND create_date < '2024-01-20';
```

### 谓词下推

```sql
-- 过滤条件下推
SELECT * FROM users
WHERE age >= 25 AND status = 'active';
```

### 压缩策略

```sql
-- 自动压缩
ALTER TABLE users SET ('compaction.min.file-num' = '5');

-- 手动压缩
CALL paimon.system.compact('database.table');

-- 大压缩
CALL paimon.system.compact('database.table', 'all');
```

### 过期数据清理

```sql
-- 设置 TTL
ALTER TABLE users SET ('table.exec.state.ttl' = '7d');

-- 分区过期
ALTER TABLE orders SET ('partition.expiration-time' = '30d');
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [README.md](README.md) | 索引文档 |
