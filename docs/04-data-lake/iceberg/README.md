# Apache Iceberg 中文文档

> Iceberg 是一个开放的数据表格式，支持 Schema 演进、时间旅行和跨引擎一致性。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作指南 |

## 快速入门

```java
// Maven 依赖
<dependency>
    <groupId>org.apache.iceberg</groupId>
    <artifactId>iceberg-spark-runtime-3.4_2.12</artifactId>
    <version>1.4.0</version>
</dependency>
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     Iceberg 核心特性                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Schema 演进                         │   │
│  │  - 列添加/删除                                           │   │
│  │  - 列重命名                                              │   │
│  │  - 列类型提升                                            │   │
│  │  - 嵌套结构演进                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Partition 演进                      │   │
│  │  - 转换函数                                              │   │
│  │  - 分区变换                                              │   │
│  │  - 隐藏分区                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      时间旅行                            │   │
│  │  - Snapshot ID 查询                                     │   │
│  │  - Snapshot 时间戳查询                                  │   │
│  │  - 增量查询                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      并发控制                            │   │
│  │  - 乐观并发                                              │   │
│  │  - 冲突检测                                              │   │
│  │  - 乐观重试                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Spark 集成

```scala
import org.apache.spark.sql.iceberg.IcebergSpark

// 读取 Iceberg
val df = spark.read.format("iceberg").load("path/to/table")

// 时间旅行
val dfV1 = spark.read
  .format("iceberg")
  .option("snapshot-id", "123456789")
  .load("path/to/table")

// 追加数据
df.writeTo("path/to/table").append()

// 合并数据
import org.apache.spark.sql.functions._
val updates = df.withColumn("updated", lit(true))
spark.sql(
  s"MERGE INTO path/to/table t USING updates u ON t.id = u.id " +
  "WHEN MATCHED THEN UPDATE SET * " +
  "WHEN NOT MATCHED THEN INSERT *"
)
```

## 相关资源

- [官方文档](https://iceberg.apache.org/docs/)
- [01-architecture.md](01-architecture.md)
