# ClickHouse MergeTree 表引擎详解

## 目录

- [MergeTree 家族](#merge-tree-家族)
- [MergeTree](#merge-tree)
- [变体表引擎](#变体表引擎)
- [复制表引擎](#复制表引擎)
- [特殊表引擎](#特殊表引擎)
- [表引擎选择指南](#表引擎选择指南)

---

## MergeTree 家族

```
┌─────────────────────────────────────────────────────────────────┐
│                    MergeTree 家族                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    MergeTree                             │   │
│  │  ├── ReplacingMergeTree - 去重引擎                       │   │
│  │  ├── SummingMergeTree - 聚合引擎                         │   │
│  │  ├── AggregatingMergeTree - 预聚合引擎                   │   │
│  │  ├── CollapsingMergeTree - 折叠引擎                      │   │
│  │  ├── VersionedCollapsingMergeTree - 版本折叠            │   │
│  │  └── ReplicatedMergeTree - 复制引擎                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Log 家族                              │   │
│  │  ├── Log - 轻量日志表                                   │   │
│  │  ├── TinyLog - 小型日志表                               │   │
│  │  └── StripeLog - 条纹日志表                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Integration 家族                      │   │
│  │  ├── Kafka - Kafka 连接器                               │   │
│  │  ├── MySQL - MySQL 连接器                               │   │
│  │  ├── HDFS - HDFS 连接器                                 │   │
│  │  └── JDBC - JDBC 连接器                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## MergeTree

### 基础 MergeTree

```sql
-- 1. 基本语法
CREATE TABLE visits (
    visit_id     UInt64,
    user_id      UInt64,
    page_url     String,
    visit_time   DateTime,
    duration     UInt64,
    source       String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(visit_time)
ORDER BY (user_id, visit_time)
PRIMARY KEY (user_id, visit_time)
SETTINGS index_granularity = 8192;

-- 2. 关键参数说明
-- PARTITION BY: 分区键
-- ORDER BY: 排序键 (必需)
-- PRIMARY KEY: 主键索引 (默认等于 ORDER BY)
-- SETTINGS: 额外设置

-- 3. 分区操作
-- 查看分区
SELECT
    partition_id,
    name,
    rows,
    active,
    min_time,
    max_time
FROM system.parts
WHERE database = 'default' AND table = 'visits';

-- 合并分区
OPTIMIZE TABLE visits FINAL;

-- 删除分区
ALTER TABLE visits DROP PARTITION TO '202401';

-- 移动分区
ALTER TABLE visits MOVE PARTITION TO DISK 'ssd'
    PARTITION ID '202401';

-- 4. 数据自动合并
-- MergeTree 后台自动合并 parts
-- 合并策略:
--   - 小 parts 优先合并
--   - 同分区 parts 合并
--   - 按时间顺序排列
```

### 索引机制

```sql
-- 1. 主键索引 (Mark)
-- 稀疏索引，每 index_granularity 行一条
-- 存储格式: .mrk 文件

-- 2. 数据跳数索引 (Skip Index)
-- 粒度索引，加速特定查询
CREATE TABLE products (
    product_id   UInt64,
    category     String,
    price        Decimal(10, 2),
    created_at   DateTime
) ENGINE = MergeTree()
ORDER BY (category, product_id)
SETTINGS index_granularity = 8192;

-- 添加数据跳数索引
ALTER TABLE products
ADD INDEX category_idx (category) TYPE set(100) GRANULARITY 4;

ALTER TABLE products
ADD INDEX price_idx (price) TYPE minmax GRANULARITY 4;

ALTER TABLE products
ADD INDEX price_bloom_idx (price) TYPE bloom_filter
    GRANULARITY 4;

-- 3. 跳数索引类型
-- minmax: 最小最大值索引
-- set: 唯一值集合
-- bloom_filter: 布隆过滤器
-- tokenbf_tokenizer: 分词布隆过滤器
-- ngrambf_v1: N-gram 布隆过滤器

-- 4. 使用跳数索引
SELECT *
FROM products
WHERE category = 'electronics'
  AND price BETWEEN 100 AND 500;

-- 5. 索引选择提示
SELECT *
FROM products
WHERE price > 1000
  AND price < 5000
  AND 5000 > price AND price > 1000;  -- 顺序影响索引选择
```

---

## 变体表引擎

### ReplacingMergeTree

```sql
-- ReplacingMergeTree - 去重表引擎
-- 同一排序键只保留最后一条记录

CREATE TABLE user_sessions (
    user_id      UInt64,
    session_id   UUID,
    login_time   DateTime,
    logout_time  DateTime,
    ip_address   String,
    device_type  String
) ENGINE = ReplacingMergeTree(login_time)
PARTITION BY toYYYYMM(login_time)
ORDER BY (user_id, session_id);

-- 特点:
-- 1. 按 ORDER BY 排序键去重
-- 2. 指定列作为版本号 (REPLACE)
-- 3. 后台合并时去重
-- 4. 使用 FINAL 强制去重

-- 示例
INSERT INTO user_sessions VALUES
    (1, 's1', '2024-01-15 10:00:00', '2024-01-15 10:30:00', '192.168.1.1', 'mobile'),
    (1, 's1', '2024-01-15 10:05:00', '2024-01-15 10:35:00', '192.168.1.2', 'mobile');  -- 更新

-- 查询 (自动去重)
SELECT *
FROM user_sessions
ORDER BY user_id, session_id;

-- 强制去重
SELECT *
FROM user_sessions
FINAL;
```

### SummingMergeTree

```sql
-- SummingMergeTree - 自动聚合表引擎
-- 同一排序键自动求和

CREATE TABLE product_sales (
    product_id   UInt64,
    date         Date,
    category     String,
    price        Decimal(10, 2),
    quantity     UInt64,
    revenue      Decimal(10, 2)  -- 会被自动求和
) ENGINE = SummingMergeTree((revenue))
PARTITION BY toYYYYMM(date)
ORDER BY (product_id, date);

-- 特点:
-- 1. 指定聚合列 (Summing)
-- 2. 数字类型自动求和
-- 3. 字符串/日期取第一个
-- 4. 需要配合 ORDER BY 使用

-- 示例
INSERT INTO product_sales VALUES
    (1, '2024-01-15', 'electronics', 99.99, 10, 999.90),
    (1, '2024-01-15', 'electronics', 99.99, 5, 499.95);  -- 自动合并

-- 查询 (自动聚合)
SELECT
    product_id,
    date,
    sum(revenue) AS total_revenue,
    sum(quantity) AS total_quantity
FROM product_sales
GROUP BY product_id, date;

-- 使用 FINAL
SELECT *
FROM product_sales
FINAL;
```

### AggregatingMergeTree

```sql
-- AggregatingMergeTree - 预聚合表引擎
-- 配合 AggregateFunction 使用

-- 1. 定义聚合状态列
CREATE TABLE hourly_metrics (
    hour                 DateTime,
    metric_name          String,
    value               AggregateFunction(quantilesTiming(0.5, 0.9), UInt64),
    count               AggregateFunction(count, UInt64),
    sum                 AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, metric_name);

-- 2. 插入聚合状态
INSERT INTO hourly_metrics
SELECT
    toStartOfHour(now()),
    'page_load_time',
    quantilesTiming(0.5, 0.9)(100, 120, 130),  -- 聚合状态
    count(3),
    sum(350);

-- 3. 使用物化视图预计算
CREATE MATERIALIZED VIEW metrics_hourly
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, metric_name)
AS SELECT
    toStartOfHour(event_time) AS hour,
    metric_name,
    quantilesTiming(0.5, 0.9)(value) AS value,
    countState(*) AS count,
    sumState(value) AS sum
FROM raw_events
GROUP BY hour, metric_name;

-- 4. 查询聚合状态
SELECT
    hour,
    metric_name,
    quantilesMerge(0.5, 0.9)(value) AS p50_p90,
    countMerge(count) AS total_count,
    sumMerge(sum) AS total_sum
FROM metrics_hourly
GROUP BY hour, metric_name;
```

### CollapsingMergeTree

```sql
-- CollapsingMergeTree - 折叠表引擎
-- 使用 Sign 列折叠相反记录

CREATE TABLE account_balance (
    account_id   UInt64,
    sign         Int8,        -- 1: 收入, -1: 支出
    amount       Decimal(10, 2),
    updated_at   DateTime
) ENGINE = CollapsingMergeTree()
PARTITION BY toYYYYMM(updated_at)
ORDER BY (account_id, updated_at)
SETTINGS sign_column = 'sign';

-- 示例
INSERT INTO account_balance VALUES
    (1001, 1, 1000.00, '2024-01-15 10:00:00'),  -- 收入
    (1001, -1, 500.00, '2024-01-15 11:00:00');  -- 支出

-- 查询 (自动折叠)
SELECT
    account_id,
    sum(amount * sign) AS balance
FROM account_balance
GROUP BY account_id;

-- 使用 FINAL
SELECT *
FROM account_balance
FINAL;
```

### VersionedCollapsingMergeTree

```sql
-- VersionedCollapsingMergeTree - 版本折叠
-- 支持多版本折叠

CREATE TABLE entity_versions (
    entity_id    UInt64,
    version      UInt64,
    sign         Int8,
    data         String,
    updated_at   DateTime
) ENGINE = VersionedCollapsingMergeTree(
    toDateTime(updated_at),
    (entity_id, version)
)
ORDER BY (entity_id, version);

-- 特点:
-- 1. 按版本折叠
-- 2. 支持乱序更新
-- 3. 适用于事件溯源
```

---

## 复制表引擎

### ReplicatedMergeTree

```sql
-- ReplicatedMergeTree - 复制表引擎
-- 基于 ZooKeeper 自动复制

CREATE TABLE orders_replicated (
    order_id      UInt64,
    order_time    DateTime,
    customer_id   UInt64,
    amount        Decimal(10, 2),
    status        String
) ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{database}/orders',
    '{replica}'
)
PARTITION BY toYYYYMM(order_time)
ORDER BY (order_id);

-- ZooKeeper 路径参数:
-- /clickhouse/tables/<database>/<table> - 共享路径
-- {replica} - 副本标识符

-- 配置副本
-- 节点1: replica='replica_01'
-- 节点2: replica='replica_02'

-- 复制机制:
-- 1. Leader 节点写入
-- 2. 写入 ZooKeeper 日志
-- 3. Follower 拉取同步
-- 4. 自动故障转移
```

### Replicated 表引擎变体

```sql
-- ReplicatedSummingMergeTree
CREATE TABLE stats_replicated (
    date         Date,
    metric_name  String,
    value        Decimal(10, 2)
) ENGINE = ReplicatedSummingMergeTree(
    '/clickhouse/tables/{database}/stats',
    '{replica}'
)
ORDER BY (date, metric_name);

-- ReplicatedReplacingMergeTree
CREATE TABLE users_replicated (
    user_id     UInt64,
    name        String,
    email       String,
    updated_at  DateTime
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{database}/users',
    '{replica}'
)
ORDER BY (user_id);

-- ReplicatedAggregatingMergeTree
CREATE TABLE metrics_replicated (
    hour          DateTime,
    metric_name   String,
    value         AggregateFunction(avg, Float64)
) ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{database}/metrics',
    '{replica}'
)
ORDER BY (hour, metric_name);
```

---

## 特殊表引擎

### Log 表引擎

```sql
-- Log 引擎 - 轻量日志表
CREATE TABLE logs (
    timestamp   DateTime,
    level       String,
    message     String
) ENGINE = Log
SETTINGS index_gamma = 1;

-- 特点:
-- - 最小内存占用
-- - 顺序写入
-- - 不支持索引
-- - 适用于临时表

-- TinyLog - 更小的日志表
CREATE TABLE tiny_logs (
    id    UInt32,
    data  String
) ENGINE = TinyLog;

-- StripeLog - 条纹日志表
CREATE TABLE stripe_logs (
    id    UInt32,
    data  String
) ENGINE = StripeLog;
```

### Integration 表引擎

```sql
-- Kafka 引擎
CREATE TABLE kafka_events (
    timestamp   DateTime,
    event_type  String,
    payload     String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka1:9092,kafka2:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONEachRow';

-- MySQL 引擎
CREATE TABLE mysql_users (
    user_id   UInt64,
    name      String,
    email     String,
    created_at DateTime
) ENGINE = MySQL('mysql_host:3306', 'database', 'users', 'user', 'password');

-- HDFS 引擎
CREATE TABLE hdfs_logs (
    timestamp   DateTime,
    level       String,
    message     String
) ENGINE = HDFS('hdfs://namenode:8020/clickhouse/logs/*.tsv', 'TSV');

-- JDBC 引擎
CREATE TABLE jdbc_orders (
    order_id    UInt64,
    order_time  DateTime,
    amount      Decimal(10, 2)
) ENGINE = JDBC('jdbc:postgresql://pg_host:5432/database', 'public', 'orders');
```

### 内存表引擎

```sql
-- Memory 引擎 - 内存表
CREATE TABLE temp_data (
    id      UInt64,
    data    String
) ENGINE = Memory;

-- 特点:
-- - 数据存储在内存
-- - 重启丢失
-- - 高性能读写
-- - 适用于临时计算

-- Set 引擎 - 集合表
CREATE TABLE user_set (
    user_id UInt64
) ENGINE = Set;

-- Insert INTO user_set VALUES (1), (2), (3);

-- Dictionary 引擎 - 字典表
CREATE DICTIONARY users_dict (
    user_id UInt64,
    name    String,
    email   String
) PRIMARY KEY user_id
SOURCE(CLICKHOUSE(TABLE 'users'))
LIFETIME(MIN 3600, MAX 7200);

-- Array 引擎 - 数组表
CREATE TABLE arrays (
    arr Array(Int32)
) ENGINE = Memory;
```

---

## 表引擎选择指南

```
┌─────────────────────────────────────────────────────────────────┐
│                    表引擎选择指南                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  场景                          推荐引擎                          │
│  ───────────────────────────────────────────────────────────   │
│  通用分析表                    MergeTree                         │
│  需要去重                      ReplacingMergeTree               │
│  需要预聚合                    SummingMergeTree / AggregatingMergeTree │
│  需要版本控制                  VersionedCollapsingMergeTree     │
│  需要复制                      ReplicatedMergeTree              │
│  临时数据                      Memory / Log                      │
│  实时数据导入                  Kafka + MergeTree                 │
│  历史数据归档                  SummingMergeTree                  │
│  物化视图源                    AggregatingMergeTree              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-data-operations.md](03-data-operations.md) | 数据操作指南 |
| [04-optimization.md](04-optimization.md) | 查询优化指南 |
| [05-sql-reference.md](05-sql-reference.md) | SQL 参考 |
| [README.md](README.md) | 索引文档 |
