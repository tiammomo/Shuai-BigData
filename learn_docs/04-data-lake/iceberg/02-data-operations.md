# Apache Iceberg 数据操作

## 目录

- [快速开始](#快速开始)
- [Spark 集成](#spark-集成)
- [Flink 集成](#flink-集成)
- [数据操作](#数据操作)
- [时间旅行](#时间旅行)

---

## 快速开始

### Maven 依赖

```xml
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-spark-runtime-3.4_2.12</artifactId>
    <version>1.4.0</version>
</dependency>
```

### Spark 配置

```scala
// Spark 配置
spark.sql.catalog.spark_catalog = org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.extensions = org.apache.iceberg.spark.extensions.IcebergSparkSessionExtension

// Catalog 配置 (Hadoop Catalog)
spark.sql.catalog.local = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.type = hadoop
spark.sql.catalog.local.warehouse = /path/to/warehouse

// Catalog 配置 (Hive Catalog)
spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hive_prod.type = hive
spark.sql.catalog.hive_prod.uri = thrift://localhost:9083
```

---

## Spark 集成

### 创建表

```scala
import org.apache.iceberg.spark._

// 1. 创建表 (CTAS)
spark.sql("""
  CREATE TABLE local.db.users (
    id BIGINT,
    name STRING,
    age INT,
    email STRING
  ) USING iceberg
  PARTITIONED BY (age)
  TBLPROPERTIES ('write.format.default'='parquet')
""")

// 2. 创建表 (DDL)
spark.sql("""
  CREATE TABLE local.db.orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    created_at TIMESTAMP
  ) USING iceberg
  PARTITIONED BY (days(created_at))
  LOCATION 's3://bucket/orders'
""")

// 3. 创建表 (带属性)
spark.sql("""
  CREATE TABLE local.db.events (
    event_id BIGINT,
    event_time TIMESTAMP,
    event_type STRING,
    user_id BIGINT
  ) USING iceberg
  PARTITIONED BY (hours(event_time))
  TBLPROPERTIES (
    'write.target-file-size-bytes'='536870912',
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read'
  )
""")
```

### 数据操作

```scala
import spark.implicits._

// 1. 插入数据
spark.sql("""
  INSERT INTO local.db.users
  VALUES (1, 'Alice', 25, 'alice@email.com'),
         (2, 'Bob', 30, 'bob@email.com')
""")

// 2. 批量插入
val df = Seq(
  (3, "Charlie", 35, "charlie@email.com"),
  (4, "David", 40, "david@email.com")
).toDF("id", "name", "age", "email")

df.writeTo("local.db.users").append()

// 3. 更新数据
spark.sql("""
  UPDATE local.db.users
  SET age = age + 1
  WHERE id = 1
""")

// 4. 删除数据
spark.sql("""
  DELETE FROM local.db.users
  WHERE age < 18
""")

// 5. 合并数据 (Merge Into)
spark.sql("""
  MERGE INTO local.db.users AS target
  USING local.db.new_users AS source
  ON target.id = source.id
  WHEN MATCHED THEN
    UPDATE SET name = source.name, age = source.age
  WHEN NOT MATCHED THEN
    INSERT (id, name, age, email)
    VALUES (source.id, source.name, source.age, source.email)
""")
```

---

## Flink 集成

### 环境配置

```java
// Flink SQL
TableEnvironment env = TableEnvironment.create(...);

// 注册_catalog
env.executeSql(
    "CREATE CATALOG hadoop_catalog WITH (" +
    "  'type' = 'iceberg'," +
    "  'catalog-impl' = 'org.apache.iceberg.hadoop.HadoopCatalog'," +
    "  'warehouse' = 'hdfs://namenode:8020/iceberg/warehouse'" +
    ")"
);

env.executeSql("USE CATALOG hadoop_catalog");
```

### 表操作

```java
// 创建表
env.executeSql(
    "CREATE TABLE db.events (" +
    "  event_id BIGINT," +
    "  event_time TIMESTAMP(3)," +
    "  event_type STRING," +
    "  user_id BIGINT" +
    ") PARTITIONED BY (event_type) " +
    "WITH (" +
    "  'format-version' = '2'," +
    "  'write.target-file-size-bytes' = '134217728'" +
    ")"
);

// 插入数据
env.executeSql(
    "INSERT INTO db.events " +
    "SELECT * FROM kafka_source"
);

// 查询数据
Table result = env.sqlQuery(
    "SELECT event_type, COUNT(*) as cnt " +
    "FROM db.events " +
    "GROUP BY event_type"
);
```

---

## 数据操作

### 追加数据

```scala
// 追加单条
spark.sql("""
  INSERT INTO table VALUES (1, 'value')
""")

// 追加多条
spark.sql("""
  INSERT INTO table VALUES (1, 'a'), (2, 'b'), (3, 'c')
""")

// 从查询追加
df.writeTo("table").append()
```

### 覆盖数据

```scala
// 动态覆盖 (覆盖所有分区)
df.writeTo("table").overwrite()

// 静态覆盖 (覆盖指定分区)
df.writeTo("table").overwrite("date = '2024-01-10'")
```

### 更新数据

```scala
// SQL 更新
spark.sql("""
  UPDATE table
  SET col = value
  WHERE condition
""")

// DataFrame API 更新
import org.apache.iceberg.spark.actions._

val actions = spark.actions
actions.updateTable("table")
  .set("property", "value")
  .execute()
```

### 删除数据

```scala
// SQL 删除
spark.sql("""
  DELETE FROM table
  WHERE condition
""")

// DataFrame 删除
df.writeTo("table").empty()
```

---

## 时间旅行

### 查看历史

```scala
// 查看历史
spark.sql("""
  SELECT * FROM local.db.users HISTORY
""").show()

// 输出字段:
// - snapshot_id: 快照ID
// - parent_id: 父快照ID
// - committed_at: 提交时间
// - operation: 操作类型
// - summary: 操作摘要
```

### 查询历史版本

```scala
// 按版本查询
spark.sql("""
  SELECT * FROM local.db.users
  VERSION AS OF 12345678901234
""").show()

// 按时间查询
spark.sql("""
  SELECT * FROM local.db.users
  TIMESTAMP AS OF '2024-01-10 00:00:00'
""").show()
```

### 分支操作

```scala
// 创建分支
spark.sql("""
  ALTER TABLE local.db.users
  CREATE BRANCH branch_2024 AS OF VERSION 12345678901234
""")

// 查询分支
spark.sql("""
  SELECT * FROM local.db.users
  VERSION AS OF branch_2024
""").show()

// 删除分支
spark.sql("""
  ALTER TABLE local.db.users
  DROP BRANCH branch_2024
""")
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-spark-integration.md](03-spark-integration.md) | Spark 集成 |
| [04-flink-integration.md](04-flink-integration.md) | Flink 集成 |
| [README.md](README.md) | 索引文档 |
