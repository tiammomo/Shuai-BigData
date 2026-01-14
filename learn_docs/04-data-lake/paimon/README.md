# Apache Paimon 中文文档

> Paimon 是一个流式数据湖，基于 Apache Flink 构建，支持实时入湖和 OLAP 查询。

## 目录

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-usage.md](02-usage.md) | 使用指南 |

## 快速入门

### Maven 依赖

```xml
<!-- Flink -->
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-flink-3.4_2.12</artifactId>
    <version>1.1.0</version>
</dependency>

<!-- Spark -->
<dependency>
    <groupId>org.apache.paimon</groupId>
    <artifactId>paimon-spark-3.4_2.12</artifactId>
    <version>1.1.0</version>
</dependency>
```

### 核心特性

```
┌─────────────────────────────────────────────────────────────────┐
│                     Paimon 核心特性                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    实时入湖                               │   │
│  │  - CDC 实时同步                                          │   │
│  │  - 流式写入                                              │   │
│  │  - 增量消费                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    流批一体                               │   │
│  │  - 统一存储格式                                          │   │
│  │  - 统一 API                                              │   │
│  │  - 统一语义                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    灵活 Schema                           │   │
│  │  - Schema Evolution                                     │   │
│  │  - 主键表 / 追加表                                       │   │
│  │  - 分区裁剪                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    丰富生态                               │   │
│  │  - Flink / Spark / Trino                                │   │
│  │  - 多种存储后端                                          │   │
│  │  - 多种文件格式                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 数据模型

```
┌─────────────────────────────────────────────────────────────────┐
│                    Paimon 数据模型                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Primary Key Table (主键表)                                      │
│  - 支持 UPSERT                                                  │
│  - 按主键去重                                                    │
│  - 适合 CDC 数据                                                │
│                                                                 │
│  Append Table (追加表)                                           │
│  - 只支持 INSERT                                                 │
│  - 无主键                                                       │
│  - 适合日志/事件数据                                             │
│                                                                 │
│  Changelog Table (变更表)                                        │
│  - 保留变更记录                                                  │
│  - 支持变更捕获                                                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Flink SQL 示例

```sql
-- 创建 Paimon Catalog
CREATE CATALOG my_paimon WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs://namenode:9000/paimon/warehouse'
);

USE CATALOG my_paimon;

-- 创建主键表
CREATE TABLE my_table (
    id BIGINT,
    name STRING,
    amount INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'bucket-key' = 'id'
);

-- 创建 Append 表
CREATE TABLE append_table (
    id BIGINT,
    data STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'merge-engine' = 'first-row',
    'compaction.min.file-num' = '5'
);

-- 插入数据
INSERT INTO my_table VALUES (1, 'Alice', 100);
INSERT INTO my_table VALUES (2, 'Bob', 200);

-- 查询
SELECT * FROM my_table;
```

## 相关资源

- [官方文档](https://paimon.apache.org/learn_docs/)
- [GitHub](https://github.com/apache/paimon)
