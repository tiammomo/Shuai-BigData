# Delta Lake 数据操作

## 目录

- [快速开始](#快速开始)
- [表操作](#表操作)
- [数据操作](#数据操作)
- [时间旅行](#时间旅行)
- [Schema 演进](#schema-演进)

---

## 快速开始

### Maven 依赖

```xml
<dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-core_2.12</artifactId>
    <version>2.4.0</version>
</dependency>
```

### Spark 配置

```scala
// Spark 配置
spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog
```

---

## 表操作

### 创建表

```scala
import io.delta.tables._
import spark.implicits._

// 1. 创建 Delta 表
spark.sql("""
  CREATE TABLE delta_events (
    event_id BIGINT,
    event_time TIMESTAMP,
    event_type STRING,
    user_id BIGINT,
    amount DECIMAL(10, 2)
  )
  USING delta
  LOCATION '/path/to/delta-events'
""")

// 2. 创建表 (带分区)
spark.sql("""
  CREATE TABLE delta_orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    created_at TIMESTAMP
  )
  USING delta
  PARTITIONED BY (status)
  LOCATION '/path/to/delta-orders'
""")

// 3. 从 DataFrame 创建
df.write.format("delta").save("/path/to/delta-table")

// 4. 转换现有表为 Delta
spark.sql("""
  CONVERT TO DELTA parquet.`/path/to/parquet-table`
""")
```

### 删除表

```scala
// 删除 Delta 表 (保留数据)
spark.sql("DROP TABLE IF EXISTS delta_table")

// 删除表和数据
spark.sql("DROP TABLE delta_table")
```

---

## 数据操作

### 插入数据

```scala
// 1. 追加数据
df.write.format("delta").mode("append").save("/path/to/delta-table")

// 2. 覆盖数据
df.write.format("delta").mode("overwrite").save("/path/to/delta-table")

// 3. 覆盖指定分区
df.write.format("delta")
  .mode("overwrite")
  .option("replaceWhere", "date = '2024-01-10'")
  .save("/path/to/delta-table")
```

### UPSERT (Merge)

```scala
val deltaTable = DeltaTable.forPath(spark, "/path/to/delta-table")

// 合并数据
deltaTable.as("target").merge(
    df.as("source"),
    "target.event_id = source.event_id"
  ).whenMatched.updateAll()
  .whenNotMatched.insertAll()
  .execute()

// 条件更新
deltaTable.updateExpr(
    Map("status" -> "completed"),
    "status = 'pending'"
)

// 删除数据
deltaTable.delete("status = 'cancelled'")
```

### 更新数据

```scala
// 使用表达式更新
deltaTable.update(
    Map("amount" -> col("amount") * 1.1),
    "status = 'pending'"
)

// 使用 SQL 表达式
deltaTable.updateExpr(
    Map("discount" -> "amount * 0.9"),
    "category = 'electronics'"
)
```

---

## 时间旅行

### 查看历史

```scala
// 查看表历史
spark.sql("""
  DESCRIBE HISTORY delta.`/path/to/delta-table`
""").show()

// 输出字段:
// - version: 版本号
// - timestamp: 操作时间
// - operation: 操作类型
// - operationParameters: 操作参数
```

### 查询历史版本

```scala
// 按版本查询
spark.read.format("delta")
  .option("versionAsOf", 5)
  .load("/path/to/delta-table")

// 按时间查询
spark.read.format("delta")
  .option("timestampAsOf", "2024-01-10 00:00:00")
  .load("/path/to/delta-table")

// 查询多个版本
spark.read.format("delta")
  .option("versionAsOf", 5)
  .load("/path/to/delta-table")
```

---

## Schema 演进

### 添加列

```scala
// 自动演进
df.write.format("delta")
  .mode("overwrite")
  .option("mergeSchema", "true")
  .save("/path/to/delta-table")

// 手动添加
spark.sql("""
  ALTER TABLE delta_table
  ADD COLUMNS (new_column STRING AFTER existing_column)
""")
```

### 变更列

```scala
// 变更列类型
spark.sql("""
  ALTER TABLE delta_table
  ALTER COLUMN amount TYPE DECIMAL(12, 2)
""")
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-spark-integration.md](03-spark-integration.md) | Spark 集成 |
| [README.md](README.md) | 索引文档 |
