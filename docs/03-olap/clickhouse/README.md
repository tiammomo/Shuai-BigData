# ClickHouse 中文文档

> ClickHouse 是一个面向列式存储的分析型数据库管理系统 (OLAP)。

## 文档列表

| 文档 | 说明 | 关键内容 |
|------|------|---------|
| [01-architecture.md](01-architecture.md) | 架构详解 | 列式存储、MergeTree、复制、分区 |
| [02-merge-tree.md](02-merge-tree.md) | 表引擎详解 | MergeTree 家族、Replicated、特殊引擎 |
| [03-data-operations.md](03-data-operations.md) | 数据操作 | INSERT、查询、UPDATE/DELETE、物化视图 |
| [04-optimization.md](04-optimization.md) | 查询优化 | 索引优化、查询改写、资源管理 |
| [05-sql-reference.md](05-sql-reference.md) | SQL 参考 | 函数、数据类型、DDL、DML |

## 快速入门

### 核心特性

```
┌─────────────────────────────────────────────────────────────────┐
│                    ClickHouse 核心特性                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ✓ 列式存储 - 高效压缩和分析查询                                 │
│  ✓ 向量化执行 - SIMD 指令优化                                    │
│  ✓ 分布式架构 - 水平扩展                                         │
│  ✓ 实时分析 - 毫秒级响应                                         │
│  ✓ 物化视图 - 预计算加速                                         │
│  ✓ 丰富函数 - SQL 扩展支持                                       │
│                                                                 │
│  性能对比 (10亿行聚合查询):                                      │
│                                                                 │
│  | 数据库      | 查询时间   | 压缩比  |                          │
│  |------------|-----------|---------|                          │
│  | ClickHouse | ~0.05s   | 8x     |                          │
│  | MySQL      | ~10s     | 1x     |                          │
│  | PostgreSQL | ~8s      | 1.5x   |                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 核心概念

```sql
-- 核心组件
-- Database - 逻辑命名空间
-- Table - 数据表
-- MergeTree - 核心表引擎
-- Partition - 数据分区
-- Replica - 数据副本

-- 创建表示例
CREATE TABLE analytics.events (
    event_id     UUID DEFAULT generateUUIDv4(),
    event_time   DateTime,
    event_type   String,
    user_id      String,
    properties   Map(String, String),
    tags         Array(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time)
PRIMARY KEY (event_type, event_time)
TTL event_time + INTERVAL 3 MONTH;
```

## 文档导航

### 入门

1. [01-architecture.md](01-architecture.md) - 了解核心架构
2. [02-merge-tree.md](02-merge-tree.md) - 学习表引擎
3. [03-data-operations.md](03-data-operations.md) - 掌握数据操作

### 进阶

4. [04-optimization.md](04-optimization.md) - 优化查询性能
5. [05-sql-reference.md](05-sql-reference.md) - SQL 参考手册

## 版本信息

| ClickHouse 版本 | 发布时间 | 特性 |
|----------------|---------|------|
| 24.3 | 2024-03 | 物化视图增强、查询优化 |
| 23.12 | 2023-12 | 并行查询、内存优化 |
| 23.8 | 2023-08 | 新函数、复制改进 |

## 相关资源

- [官方文档](https://clickhouse.com/docs/)
