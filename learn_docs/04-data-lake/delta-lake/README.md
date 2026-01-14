# Delta Lake 中文文档

> Delta Lake 是一个开源的存储层，提供 ACID 事务能力。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作指南 |
| [03-spark-integration.md](03-spark-integration.md) | Spark 集成 |

## 快速入门

```java
// Maven 依赖
<dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-core_2.12</artifactId>
    <version>2.4.0</version>
</dependency>
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     Delta Lake 核心特性                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      ACID 事务                           │   │
│  │  - 原子性保证                                            │   │
│  │  - 一致性保证                                            │   │
│  │  - 隔离性保证                                            │   │
│  │  - 持久性保证                                            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Schema 演进                         │   │
│  │  - 列添加/删除                                           │   │
│  │  - 列类型变更                                            │   │
│  │  - 约束添加                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      时间旅行                            │   │
│  │  - 版本历史                                              │   │
│  │  - 历史回溯                                              │   │
│  │  - 快照查询                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      数据版本管理                         │   │
│  │  - 增量写入                                              │   │
│  │  - 合并/更新/删除                                        │   │
│  │  - 真空清理                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Spark 集成

```scala
import io.delta.tables._
import org.apache.spark.sql.functions._

// 读取 Delta Lake
val df = spark.read.format("delta").load("/path/to/delta")

// 写入 Delta Lake
df.write.format("delta").mode("overwrite").save("/path/to/delta")

// 时间旅行查询
val dfV1 = spark.read.format("delta").option("versionAsOf", 1).load("/path/to/delta")
val dfTs = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("/path/to/delta")
```

## 相关资源

- [官方文档](https://docs.delta.io/)
