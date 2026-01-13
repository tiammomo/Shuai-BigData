# ClickHouse 架构详解

## 目录

- [核心概念](#核心概念)
- [存储架构](#存储架构)
- [复制机制](#复制机制)
- [分区机制](#分区机制)
- [查询处理](#查询处理)
- [性能优化](#性能优化)

---

## 核心概念

### ClickHouse 简介

```
┌─────────────────────────────────────────────────────────────────┐
│                    ClickHouse 简介                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ClickHouse 是一个面向列式存储的分析型数据库管理系统 (OLAP):      │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ✓ 列式存储 - 高效压缩和分析查询                          │   │
│  │  ✓ 向量化执行 - SIMD 指令优化                             │   │
│  │  ✓ 分布式架构 - 水平扩展                                  │   │
│  │  ✓ 实时分析 - 毫秒级响应                                  │   │
│  │  ✓ 物化视图 - 预计算加速                                  │   │
│  │  ✓ 近似计算 - 采样和草图技术                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  vs 其他 OLAP 数据库:                                           │
│                                                                 │
│  | 特性        | ClickHouse | Druid  | Kylin  | Presto |      │
│  |------------|------------|--------|--------|--------|      │
│  | 存储模型    | 列式       | 列式   | 预计算 | 虚拟   |      │
│  | 查询延迟    | 毫秒级     | 秒级   | 秒级   | 秒级   |      │
│  | 数据量      | PB 级      | PB 级  | TB 级  | PB 级  |      │
│  | 实时写入    | 支持       | 支持   | 不支持 | 支持   |      │
│  | SQL 支持    | 完全       | 部分   | 部分   | 完全   |      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 核心术语

```sql
-- ClickHouse 核心术语

-- 1. Database (数据库)
-- 逻辑命名空间，包含表
CREATE DATABASE analytics;

-- 2. Table (表)
-- 数据存储的基本单位
CREATE TABLE events (
    event_id    UUID DEFAULT generateUUIDv4(),
    event_time  DateTime,
    event_type  String,
    user_id     String,
    properties  Map(String, String),
    tags        Array(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time);

-- 3. MergeTree 家族
-- MergeTree - 基础表引擎
-- ReplacingMergeTree - 去重
-- SummingMergeTree - 自动聚合
-- AggregatingMergeTree - 预聚合
-- CollapsingMergeTree - 折叠
-- VersionedCollapsingMergeTree - 版本折叠
-- ReplicatedMergeTree - 复制表

-- 4. Partition (分区)
-- 数据物理分区，按分区键划分

-- 5. Part (数据 part)
-- 分区的物理单元，存储数据文件

-- 6. Block (数据块)
-- 内存中的数据单元，64KB-256KB

-- 7. Column (列)
-- 物理存储的列数据

-- 8. Primary Key (主键)
-- 索引键，用于快速定位

-- 9. Order Key (排序键)
-- 数据排序键

-- 10. Index Granularity (索引粒度)
-- 索引间隔，默认 8192 行
```

---

## 存储架构

### 数据存储结构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    ClickHouse 数据存储结构                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  目录结构:                                                               │
│  /var/lib/clickhouse/                                                   │
│  ├── data/                                                              │
│  │   └── <database>/                                                    │
│  │       └── <table>/                                                   │
│  │           └── <partition_id>/                                        │
│  │               ├── <min_block_num>_<max_block_num>_1/                 │
│  │               │   ├── checksums.txt                                  │
│  │               │   ├── columns.txt                                    │
│  │               │   ├── primary.idx                                    │
│  │               │   ├──.bin (数据文件)                                  │
│  │               │   ├── mrk (标记文件)                                  │
│  │               │   └── partition.idx                                  │
│  │               └── ...                                                 │
│  └── metadata/                                                          │
│       └── <database>/                                                    │
│           └── <table>.sql                                                │
│                                                                          │
│  数据文件格式:                                                           │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  Column A      Column B      Column C      ...                  │    │
│  │  ┌────────┐   ┌────────┐   ┌────────┐                         │    │
│  │  │ A1     │   │ B1     │   │ C1     │  ← Block 1 (8192行)      │    │
│  │  │ A2     │   │ B2     │   │ C2     │                         │    │
│  │  │ ...    │   │ ...    │   │ ...    │                         │    │
│  │  │ A8192  │   │ B8192  │   │ C8192  │                         │    │
│  │  └────────┘   └────────┘   └────────┘                         │    │
│  │                                                                │    │
│  │  ┌────────┐   ┌────────┐   ┌────────┐                         │    │
│  │  │ A8193  │   │ B8193  │   │ C8193  │  ← Block 2              │    │
│  │  │ ...    │   │ ...    │   │ ...    │                         │    │
│  │  └────────┘   └────────┘   └────────┘                         │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 列式存储优势

```sql
-- 列式存储 vs 行式存储对比

-- 行式存储 (传统数据库)
-- ┌──────────────────────────────────────┐
-- │ (id, name, age, city)                │
-- │ [1, Alice, 25, Beijing]              │
-- │ [2, Bob, 30, Shanghai]               │
-- │ [3, Carol, 28, Shenzhen]             │
-- └──────────────────────────────────────┘

-- 列式存储 (ClickHouse)
-- ┌──────────────────────────────────────┐
-- │ Column: id    → [1, 2, 3]            │
-- │ Column: name  → [Alice, Bob, Carol]  │
-- │ Column: age   → [25, 30, 28]         │
-- │ Column: city  → [Beijing, Shanghai,  │
-- │                      Shenzhen]       │
-- └──────────────────────────────────────┘

-- 示例: 计算平均年龄
-- 行式存储: 读取所有行，提取 age 列，计算平均
-- 列式存储: 只读取 age 列数据，直接计算

-- 列式存储优势:
-- 1. 只读取需要的列，减少 IO
-- 2. 同类型数据压缩率高
-- 3. 便于向量化执行
-- 4. 列统计便于优化
```

### 向量化执行

```sql
-- 向量化执行示例

-- 传统行式执行
-- for each row:
--     parse columns
--     compute expression
--     output result

-- 向量化执行
-- for each block (8192 rows):
--     load columns into SIMD registers
--     apply operation to entire register
--     output results for block

-- 示例: 计算折扣后价格
-- 非向量化:
SELECT
    product_id,
    price * (1 - discount) AS final_price
FROM sales;

-- 向量化优化:
-- 1. 加载 price 列到内存块
-- 2. 加载 discount 列到内存块
-- 3. 使用 SIMD 指令并行计算
-- 4. 输出结果块
```

---

## 复制机制

### ReplicatedMergeTree

```sql
-- 复制表配置

-- 1. ZooKeeper 复制
CREATE TABLE orders_replicated (
    order_id    UInt64,
    order_time  DateTime,
    customer_id String,
    amount      Decimal(10, 2),
    status      String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{database}/{table}',
    '{replica}'
)
PARTITION BY toYYYYMM(order_time)
ORDER BY (order_id);

-- 2. 复制机制说明
-- - ZooKeeper 协调复制
-- -  Leader 负责写入
-- -  Follower 同步数据
-- - 自动故障转移

-- 3. 复制流程
-- ┌─────────────────────────────────────────────────────────────┐
-- │  Client                                                      │
-- │     │                                                        │
-- │     ▼                                                        │
-- │  ┌─────────────────────────────────────────────────────────┐│
-- │  │  ClickHouse Node 1 (Leader)                              ││
-- │  │  - 接收 INSERT                                          ││
-- │  │  - 写入本地 part                                         ││
-- │  │  - 写 ZooKeeper 日志                                    ││
-- │  └─────────────────────────────────────────────────────────┘│
-- │     │                                                        │
--     ▼                                                        │
--  ┌─────────────────────────────────────────────────────────────┐
--  │  ZooKeeper                                                  │
--  │  - 记录复制任务                                             │
--  │  - 协调节点                                                 │
--  └─────────────────────────────────────────────────────────────┘
--     │         │         │
--     ▼         ▼         ▼
--  ┌─────┐  ┌─────┐  ┌─────┐
--  │Replica1│ │Replica2│ │Replica3│
--  │(同步)  │ │(同步)  │ │(异步)  │
--  └─────┘  └─────┘  └─────┘
```

### 复制配置

```xml
<!-- config.xml -->
<zookeeper>
    <node>
        <host>zk1</host>
        <port>2181</port>
    </node>
    <node>
        <host>zk2</host>
        <port>2181</port>
    </node>
    <node>
        <host>zk3</host>
        <port>2181</port>
    </node>
</zookeeper>

<!-- 复制配置 -->
<default_replication_factor>2</default_replication_factor>
<max_replication_delay_for_inserts>60</max_replication_delay_for_inserts>
```

---

## 分区机制

### 分区策略

```sql
-- 分区键选择

-- 1. 按时间分区 (最常用)
CREATE TABLE events_time (
    event_time  DateTime,
    event_type  String,
    data        String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time);

-- 2. 按日期分区
CREATE TABLE events_date (
    event_date  Date,
    event_type  String,
    data        String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (event_date, event_type);

-- 3. 按枚举值分区
CREATE TABLE events_enum (
    event_type  Enum8('error' = 1, 'warning' = 2, 'info' = 3),
    count       UInt64
) ENGINE = MergeTree()
PARTITION BY event_type
ORDER BY (event_type);

-- 4. 多级分区
CREATE TABLE events_multi (
    year        UInt16,
    month       UInt8,
    day         UInt8,
    event_type  String
) ENGINE = MergeTree()
PARTITION BY (year, month)
ORDER BY (year, month, day, event_type);

-- 5. 分区操作
-- 查看分区
SELECT
    partition_id,
    name,
    rows,
    active
FROM system.parts
WHERE database = 'default' AND table = 'events';

-- 删除分区
ALTER TABLE events DROP PARTITION toYYYYMM(toDateTime('2024-01-15'));

-- 移动分区
ALTER TABLE events MOVE PARTITION toYYYYMM(toDateTime('2024-01-15'))
    TO DISK 'slow_disk';

-- 克隆分区
ALTER TABLE events CLONE PARTITION toYYYYMM(toDateTime('2024-01-15'))
    FROM TABLE events_backup;
```

---

## 查询处理

### 查询流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      ClickHouse 查询流程                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. Parser (解析)                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  SQL 字符串 → 抽象语法树 (AST)                                    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  2. Interpreter (解释)                                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  AST → 查询执行计划                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  3. Query Planner (规划)                                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  优化查询计划                                                     │   │
│  │  - 列裁剪                                                        │   │
│  │  - 谓词下推                                                      │   │
│  │  - 索引选择                                                      │   │
│  │  - 并行化                                                        │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  4. Pipeline Builder (管道构建)                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  构建执行管道                                                     │   │
│  │  - Source → Transform → Aggregator → Sink                       │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  5. Execution (执行)                                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  并行执行管道                                                     │   │
│  │  - 多线程                                                        │   │
│  │  - SIMD 向量化                                                   │   │
│  │  - 分布式协调                                                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 查询优化

```sql
-- 查询优化技巧

-- 1. 使用 PREWHERE
-- 提前过滤数据，减少读取
SELECT count()
FROM visits
PREWHERE
    toStartOfMonth(visit_date) = '2024-01-01'
WHERE
    user_id = 12345;

-- 2. 使用 FINAL (慎用)
-- 自动去重，但有性能开销
SELECT count()
FROM events FINAL
WHERE event_type = 'pageview';

-- 3. 优化 JOIN
-- 小表放前面 (左表)
SELECT
    o.order_id,
    c.name
FROM orders o
    ANY LEFT JOIN customers c ON o.customer_id = c.id;

-- 4. 使用物化视图
CREATE MATERIALIZED VIEW hourly_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, event_type)
AS SELECT
    toStartOfHour(event_time) AS hour,
    event_type,
    count() AS cnt,
    uniqExact(user_id) AS unique_users
FROM events
GROUP BY hour, event_type;

-- 5. 使用采样
SELECT avg(price)
FROM purchases
SAMPLE 0.1  -- 10% 采样
WHERE product_type = 'electronics';
```

---

## 性能优化

### 索引优化

```sql
-- 1. 主键选择
-- 高基数列放前面
CREATE TABLE orders (
    order_id    UInt64,
    customer_id UInt64,
    order_time  DateTime,
    amount      Decimal(10, 2),
    status      String
) ENGINE = MergeTree()
ORDER BY (order_id, customer_id, order_time);  -- 低基数放前

-- 2. 数据跳数索引 (Skiplist Index)
CREATE TABLE visits (
    visit_id    UInt64,
    user_id     UInt64,
    page_url    String,
    visit_time  DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, visit_time)
SETTINGS index_granularity = 8192;

-- 添加跳数索引
ALTER TABLE visits
ADD INDEX user_idx (user_id) TYPE set(100) GRANULARITY 4;

-- 3. 物化主键
-- 用于复杂的预计算
CREATE TABLE agg_metrics (
    date        Date,
    metric_name String,
    value       AggregateFunction(quantilesTiming(0.5, 0.9), UInt64)
) ENGINE = SummingMergeTree()
ORDER BY (date, metric_name);

-- 4. 分区裁剪
-- 最佳实践: 只查询必要分区
SELECT *
FROM events
PREWHERE
    event_date >= '2024-01-01'
    AND event_date < '2024-02-01';
```

### 写入优化

```sql
-- 写入优化

-- 1. 批量写入
-- 每次写入 10,000+ 行
INSERT INTO events
VALUES
    (now(), 'click', 'page1'),
    (now(), 'view', 'page2'),
    ...;

-- 2. 使用 Buffer 表
CREATE TABLE events_buffer (
    event_time  DateTime,
    event_type  String,
    data        String
) ENGINE = Buffer(
    default, events,
    16, 10, 100, 10000, 1000000, 10000000, 100000000, 1000000000
);

-- 3. 禁用异步插入
SET async_insert = 0;

-- 4. 控制写入频率
SET insert_block_size = 1048576;

-- 5. 使用 Distributed 引擎
INSERT INTO distributed_events
SELECT * FROM events_buffer;
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [02-merge-tree.md](02-merge-tree.md) | MergeTree 表引擎详解 |
| [03-data-operations.md](03-data-operations.md) | 数据操作指南 |
| [04-optimization.md](04-optimization.md) | 查询优化指南 |
| [05-sql-reference.md](05-sql-reference.md) | SQL 参考 |
| [README.md](README.md) | 索引文档 |
