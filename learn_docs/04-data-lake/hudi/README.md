# Apache Hudi 中文文档

> Hudi 是一个开源的数据湖框架，提供增量处理和事务能力。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作指南 |

## 快速入门

```java
// Maven 依赖
<dependency>
    <groupId>org.apache.hudi</groupId>
    <artifactId>hudi-spark-bundle_2.12</artifactId>
    <version>0.14.0</version>
</dependency>
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     Hudi 核心架构                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      表类型                              │   │
│  │  ┌─────────────────────────────────────────────────┐   │   │
│  │  │  Copy On Write (COW)                            │   │   │
│  │  │  - 写时复制                                      │   │   │
│  │  │  - 读取优化                                      │   │   │
│  │  │  - 适合读多写少场景                              │   │   │
│  │  └─────────────────────────────────────────────────┘   │   │
│  │  ┌─────────────────────────────────────────────────┐   │   │
│  │  │  Merge On Read (MOR)                            │   │   │
│  │  │  - 读时合并                                      │   │   │
│  │  │  - 写时优化                                      │   │   │
│  │  │  - 适合写多读少场景                              │   │   │
│  │  └─────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      查询类型                            │   │
│  │  - Snapshot Query - 快照查询                            │   │
│  │  - Incremental Query - 增量查询                         │   │
│  │  - Read Optimized Query - 读优化查询                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Spark 集成

```scala
import org.apache.spark.sql.SaveMode
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig

// 写入 Hudi
df.write
  .format("hudi")
  .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, "COPY_ON_WRITE")
  .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "id")
  .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "date")
  .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
  .mode(SaveMode.Overwrite)
  .save("/path/to/hudi/table")
```

## 相关资源

- [官方文档](https://hudi.apache.org/learn_docs/)
- [01-architecture.md](01-architecture.md)
