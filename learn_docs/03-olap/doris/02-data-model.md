# Doris 数据模型详解

## 目录

- [数据模型概述](#数据模型概述)
- [Unique 模型](#unique-模型)
- [Aggregate 模型](#aggregate-模型)
- [Duplicate 模型](#duplicate-模型)
- [分区设计](#分区设计)
- [分桶设计](#分桶设计)
- [物化视图](#物化视图)

---

## 数据模型概述

### 模型类型对比

```
┌─────────────────────────────────────────────────────────────────┐
│                    Doris 数据模型                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Unique 模型                         │   │
│  │  - 唯一键约束                                            │   │
│  │  - 支持更新操作                                          │   │
│  │  - 主键索引                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Aggregate 模型                        │   │
│  │  - 预聚合模型                                            │   │
│  │  - 自动聚合相同 Key                                      │   │
│  │  - 减少存储和查询计算                                     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Duplicate 模型                        │   │
│  │  - 明细模型                                              │   │
│  │  - 保留所有数据                                          │   │
│  │  - 最灵活的数据模型                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  选择指南:                                                        │
│                                                                 │
│  | 场景                    | 推荐模型      | 原因              │   │
│  |------------------------|--------------|------------------│   │
│  | 需要唯一性约束          | Unique       | 主键唯一          │   │
│  | 需要实时更新            | Unique       | 支持 upsert      │   │
│  | 需要预聚合统计          | Aggregate    | 自动聚合          │   │
│  | 明细数据分析            | Duplicate    | 保留所有明细      │   │
│  | 日志/事件数据           | Duplicate    | 原始数据保留      │   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Unique 模型

### 基础语法

```sql
-- Unique 模型特点:
-- 1. 唯一键约束 - 相同唯一键只保留最新值
-- 2. 支持 UPDATE/DELETE 操作
-- 3. 支持主键索引

-- 创建 Unique 模型表
CREATE TABLE users (
    user_id     BIGINT      NOT NULL,
    username    VARCHAR(50) NOT NULL,
    email       VARCHAR(100),
    age         INT,
    create_time DATETIME    DEFAULT CURRENT_TIMESTAMP
) UNIQUE KEY(user_id, username)
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true"
);

-- 查看表结构
DESC users;

-- 显示建表语句
SHOW CREATE TABLE users;
```

### Merge-on-Write vs Merge-on-Read

```sql
-- 1. Merge-on-Write (写时合并) - 推荐
-- 写入时立即去重，读取时无需合并
-- 适合写少读多的场景

CREATE TABLE orders (
    order_id    BIGINT      NOT NULL,
    user_id     BIGINT,
    status      VARCHAR(20),
    amount      DECIMAL(10, 2),
    update_time DATETIME
) UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true"
);

-- 2. Merge-on-Read (读时合并) - 已弃用
-- 写入时不合并，读取时合并
-- 适合写多读少的场景

CREATE TABLE old_table (
    order_id    BIGINT NOT NULL,
    amount      DECIMAL(10, 2)
) UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "false"  -- 旧模式
);
```

### 更新操作

```sql
-- 1. INSERT (主键存在则更新，不存在则插入)
INSERT INTO users (user_id, username, email, age)
VALUES (1001, 'alice', 'alice@example.com', 25)
ON DUPLICATE KEY UPDATE
    email = 'alice@example.com',
    age = 25;

-- 2. INSERT IGNORE (主键存在则忽略)
INSERT IGNORE INTO users (user_id, username, email, age)
VALUES (1001, 'alice', 'alice@example.com', 25);

-- 3. REPLACE (主键存在则替换)
REPLACE INTO users (user_id, username, email, age)
VALUES (1001, 'alice', 'alice@new.com', 26);

-- 4. UPDATE
UPDATE users
SET email = 'alice@new.com', age = 26
WHERE user_id = 1001;

-- 5. DELETE
DELETE FROM users WHERE user_id = 1001;
```

---

## Aggregate 模型

### 聚合函数

```sql
-- Aggregate 模型特点:
-- 1. 按 Key 聚合
-- 2. 支持多种聚合函数
-- 3. 减少存储空间

-- 创建 Aggregate 模型表
CREATE TABLE sales (
    order_id      BIGINT      NOT NULL,
    product_id    BIGINT      NOT NULL,
    user_id       BIGINT,
    amount        SUM         DEFAULT 0,
    quantity      SUM         DEFAULT 0,
    revenue       SUM         DEFAULT 0,
    order_time    DATETIME,
    product_name  VARCHAR(100)
) AGGREGATE KEY(order_id, product_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3"
);

-- 聚合函数说明:
-- SUM   - 求和
-- MAX   - 最大值
-- MIN   - 最小值
-- REPLACE    - 替换 (后值覆盖前值)
-- REPLACE_IF_NOT_NULL  - 非空时替换
-- HLL_UNION - HyperLogLog 聚合
-- BITMAP_UNION - 位图聚合

-- 聚合示例:
-- 相同 (order_id, product_id) 的记录自动聚合
-- amount = SUM(amount)
-- quantity = SUM(quantity)
-- product_name = REPLACE(product_name)
```

### 多指标聚合

```sql
-- 创建多指标聚合表
CREATE TABLE stats (
    stat_date    DATE        NOT NULL,
    metric_name  VARCHAR(50) NOT NULL,
    metric_value SUM         DEFAULT 0,
    count        SUM         DEFAULT 0,
    min_value    MIN         DEFAULT NULL,
    max_value    MAX         DEFAULT NULL
) AGGREGATE KEY(stat_date, metric_name)
DISTRIBUTED BY HASH(stat_date) BUCKETS 4
PROPERTIES (
    "replication_num" = "3"
);

-- 插入数据
INSERT INTO stats VALUES
    ('2024-01-15', 'page_views', 1000, 100, 1, 100),
    ('2024-01-15', 'page_views', 500, 50, 1, 100);

-- 查询时自动聚合
SELECT stat_date, metric_name, SUM(metric_value), SUM(count)
FROM stats
WHERE stat_date = '2024-01-15'
GROUP BY stat_date, metric_name;
-- 结果: stat_date='2024-01-15', metric_name='page_views', metric_value=1500, count=150
```

### 位图聚合

```sql
-- 创建位图表
CREATE TABLE user_behavior (
    event_id      BIGINT      NOT NULL,
    user_id       BIGINT,
    behavior_type VARCHAR(20),
    date          DATE,
    user_bitmap   BITMAP_UNION
) AGGREGATE KEY(event_id, user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3"
);

-- 插入位图数据
INSERT INTO user_behavior
SELECT 1, 1001, 'click', '2024-01-15', BITMAP_EMPTY()
UNION ALL
SELECT 2, 1002, 'click', '2024-01-15', BITMAP_EMPTY();

-- 使用位图函数
SELECT
    date,
    behavior_type,
    BITMAP_COUNT(USER_BITMAP) as unique_users
FROM user_behavior
GROUP BY date, behavior_type;

-- 使用 HLL 近似计算
CREATE TABLE metrics (
    metric_name VARCHAR(50),
    users       HLL_UNION
) AGGREGATE KEY(metric_name)
DISTRIBUTED BY HASH(metric_name) BUCKETS 4;
```

---

## Duplicate 模型

### 基础用法

```sql
-- Duplicate 模型特点:
-- 1. 无聚合，所有数据原样存储
-- 2. 适合明细数据分析
-- 3. 支持所有分析查询

-- 创建 Duplicate 模型表
CREATE TABLE events (
    event_id      BIGINT      NOT NULL,
    event_time    DATETIME,
    event_type    VARCHAR(50),
    user_id       BIGINT,
    page_url      VARCHAR(500),
    ip_address    VARCHAR(50),
    device_type   VARCHAR(20)
) DUPLICATE KEY(event_id, event_time)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3"
);

-- 插入数据
INSERT INTO events VALUES
    (1, '2024-01-15 10:00:00', 'page_view', 1001, '/home', '192.168.1.1', 'mobile'),
    (2, '2024-01-15 10:00:01', 'click', 1001, '/home', '192.168.1.1', 'mobile'),
    (3, '2024-01-15 10:00:02', 'page_view', 1002, '/product', '192.168.1.2', 'desktop');

-- 所有记录都会被保留
SELECT * FROM events;
-- 返回 3 条记录
```

### 日志数据场景

```sql
-- 典型日志表设计
CREATE TABLE access_logs (
    id            BIGINT      NOT NULL AUTO_INCREMENT,
    log_time      DATETIME(3) NOT NULL,
    client_ip     VARCHAR(50),
    method        VARCHAR(20),
    url           VARCHAR(500),
    status_code   INT,
    response_time INT,
    user_agent    VARCHAR(500)
) DUPLICATE KEY(id, log_time)
PARTITION BY RANGE(log_time) (
    PARTITION p202401 VALUES LESS THAN ('2024-02-01'),
    PARTITION p202402 VALUES LESS THAN ('2024-03-01'),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
)
DISTRIBUTED BY HASH(id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true"
);

-- 按需查询明细
SELECT
    DATE(log_time) as log_date,
    COUNT(*) as pv,
    AVG(response_time) as avg_response_time
FROM access_logs
WHERE log_time >= '2024-01-15'
GROUP BY DATE(log_time);
```

---

## 分区设计

### Range 分区

```sql
-- 1. 按日期分区
CREATE TABLE orders (
    order_id     BIGINT      NOT NULL,
    order_time   DATETIME    NOT NULL,
    customer_id  BIGINT,
    amount       DECIMAL(10, 2)
) UNIQUE KEY(order_id)
PARTITION BY RANGE(order_time) (
    PARTITION p202401 VALUES LESS THAN ('2024-02-01'),
    PARTITION p202402 VALUES LESS THAN ('2024-03-01'),
    PARTITION p202403 VALUES LESS THAN ('2024-04-01'),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES ("replication_num" = "3");

-- 2. 按月份自动分区
CREATE TABLE monthly_stats (
    stat_date    DATE        NOT NULL,
    metric_name  VARCHAR(50),
    value        BIGINT
) UNIQUE KEY(stat_date, metric_name)
PARTITION BY RANGE(TO_DAYS(stat_date)) (
    PARTITION p202401 VALUES LESS THAN (TO_DAYS('2024-02-01')),
    PARTITION p202402 VALUES LESS THAN (TO_DAYS('2024-03-01')),
    PARTITION p202403 VALUES LESS THAN (TO_DAYS('2024-04-01'))
)
DISTRIBUTED BY HASH(stat_date) BUCKETS 4
PROPERTIES ("replication_num" = "3");

-- 3. 分区操作
-- 添加分区
ALTER TABLE orders ADD PARTITION p202404
VALUES LESS THAN ('2024-05-01');

-- 删除分区
ALTER TABLE orders DROP PARTITION p202401;

-- 分裂分区
ALTER TABLE orders SPLIT PARTITION pmax
INTO (
    PARTITION p202404 VALUES LESS THAN ('2024-05-01'),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);

-- 查看分区
SHOW PARTITIONS FROM orders;
```

### List 分区

```sql
-- 按地区分区
CREATE TABLE users (
    user_id   BIGINT      NOT NULL,
    name      VARCHAR(50),
    region    VARCHAR(20)
) UNIQUE KEY(user_id)
PARTITION BY LIST(region) (
    PARTITION p_north VALUES IN ('北京', '天津', '河北', '山西'),
    PARTITION p_south VALUES IN ('上海', '江苏', '浙江', '安徽'),
    PARTITION p_west VALUES IN ('四川', '重庆', '云南', '贵州'),
    PARTITION p_other VALUES IN (DEFAULT)
)
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES ("replication_num" = "3");

-- 多列分区
CREATE TABLE sales (
    sale_id    BIGINT NOT NULL,
    year       INT,
    month      INT,
    amount     DECIMAL(10, 2)
) UNIQUE KEY(sale_id)
PARTITION BY LIST(year, month) (
    PARTITION p_2024_01 VALUES IN ((2024, 1)),
    PARTITION p_2024_02 VALUES IN ((2024, 2))
)
DISTRIBUTED BY HASH(sale_id) BUCKETS 4
PROPERTIES ("replication_num" = "3");
```

### 分区最佳实践

```sql
-- 1. 分区数量控制
-- 每个表建议不超过 1000 个分区
-- 历史分区及时归档或删除

-- 2. 分区粒度选择
-- 日志数据: 按天分区
-- 业务数据: 按月分区
-- 历史归档: 按年分区

-- 3. 使用动态分区
ALTER TABLE orders SET ("enable_dynamic_partition" = "true");
ALTER TABLE orders SET ("dynamic_partition.enable" = "true");
ALTER TABLE orders SET ("dynamic_partition.history_partition_num" = "7");
ALTER TABLE orders SET ("dynamic_partition.prefix" = "p");
ALTER TABLE orders SET ("dynamic_partition.buckets" = "8");

-- 4. 查看分区信息
SELECT * FROM information_schema.partitions
WHERE table_name = 'orders';
```

---

## 分桶设计

### 分桶策略

```sql
-- 1. 选择分桶键
-- 高基数字段: 用户ID、订单ID
-- 低基数字段: 状态、类型

-- 2. 分桶数量
-- 建议: 集群 BE 节点数 * 核心数
-- 范围: 10-1000

-- 3. Hash 分桶
CREATE TABLE users (
    user_id   BIGINT NOT NULL,
    name      VARCHAR(50),
    age       INT
) UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "3");

-- 4. 复合分桶键
CREATE TABLE orders (
    order_id     BIGINT NOT NULL,
    user_id      BIGINT,
    product_id   BIGINT,
    amount       DECIMAL(10, 2)
) UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(user_id, product_id) BUCKETS 16
PROPERTIES ("replication_num" = "3");

-- 5. 分桶数量调整
-- 只能通过重建表调整
CREATE TABLE users_new (
    user_id   BIGINT NOT NULL,
    name      VARCHAR(50),
    age       INT
) UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES ("replication_num" = "3");

INSERT INTO users_new SELECT * FROM users;
DROP TABLE users;
RENAME TABLE users_new TO users;
```

### 分桶与查询

```sql
-- 1. 最佳实践: Join 字段与分桶字段一致
CREATE TABLE orders (
    order_id     BIGINT NOT NULL,
    user_id      BIGINT,
    amount       DECIMAL(10, 2)
) UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES ("replication_num" = "3");

CREATE TABLE users (
    user_id   BIGINT NOT NULL,
    name      VARCHAR(50)
) UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES ("replication_num" = "3");

-- Join 时可以本地 Shuffle，避免跨节点数据移动
SELECT * FROM orders o
JOIN users u ON o.user_id = u.user_id;

-- 2. 查看分桶分布
ADMIN SHOW REPLICA DISTRIBUTION FROM users;
```

---

## 物化视图

### 创建物化视图

```sql
-- 1. 创建聚合物化视图
CREATE MATERIALIZED VIEW mv_sales_monthly
BUILD IMMEDIATE REFRESH ON COMMIT
AS SELECT
    product_id,
    DATE_FORMAT(order_time, '%Y-%m') as month,
    SUM(amount) as total_amount,
    COUNT(*) as order_count,
    AVG(amount) as avg_amount
FROM orders
GROUP BY product_id, DATE_FORMAT(order_time, '%Y-%m');

-- 2. 创建明细物化视图
CREATE MATERIALIZED VIEW mv_orders_detail
BUILD IMMEDIATE REFRESH ON COMMIT
AS SELECT
    order_id,
    user_id,
    product_id,
    order_time,
    amount,
    status
FROM orders;

-- 3. 查看物化视图
SHOW MATERIALIZED VIEWS FROM database;

-- 4. 查看物化视图定义
SHOW CREATE MATERIALIZED VIEW mv_sales_monthly;

-- 5. 刷新物化视图
REFRESH MATERIALIZED VIEW mv_sales_monthly;
REFRESH MATERIALIZED VIEW mv_sales_monthly FORCE;

-- 6. 删除物化视图
DROP MATERIALIZED VIEW IF EXISTS mv_sales_monthly;
```

### 自动选择物化视图

```sql
-- Doris 自动选择最优物化视图
-- 查询原始表
SELECT
    product_id,
    DATE_FORMAT(order_time, '%Y-%m') as month,
    SUM(amount) as total_amount
FROM orders
WHERE order_time >= '2024-01-01'
GROUP BY product_id, DATE_FORMAT(order_time, '%Y-%m');

-- Doris 自动使用 mv_sales_monthly

-- 查看查询计划
EXPLAIN SELECT
    product_id,
    DATE_FORMAT(order_time, '%Y-%m') as month,
    SUM(amount) as total_amount
FROM orders
WHERE order_time >= '2024-01-01'
GROUP BY product_id, DATE_FORMAT(order_time, '%Y-%m');

-- 查看物化视图使用情况
SELECT * FROM information_schema.materialized_views
WHERE table_name = 'orders';
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | Doris 架构详解 |
| [03-data-operations.md](03-data-operations.md) | 数据操作指南 |
| [04-sql-reference.md](04-sql-reference.md) | SQL 参考 |
| [05-operations.md](05-operations.md) | 运维指南 |
| [README.md](README.md) | 索引文档 |
