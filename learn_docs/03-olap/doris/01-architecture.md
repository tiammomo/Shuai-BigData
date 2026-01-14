# Apache Doris 架构详解

## 目录

- [核心概念](#核心概念)
- [存储架构](#存储架构)
- [数据模型](#数据模型)
- [查询引擎](#查询引擎)
- [高可用机制](#高可用机制)
- [扩缩容机制](#扩缩容机制)

---

## 核心概念

### Doris 简介

```
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Doris 简介                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Apache Doris 是一个基于 MPP 架构的分析型数据库:                 │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ✓ MPP 架构 - 并行分布式查询                              │   │
│  │  ✓ 列式存储 - 高效压缩和分析                              │   │
│  │  ✓ 向量化执行 - SIMD 优化                                 │   │
│  │  ✓ 高可用 - FE/BE 冗余部署                                │   │
│  │  ✓ 实时更新 - 唯一键/批量更新                             │   │
│  │  ✓ 生态丰富 - Spark/Flink/BI 连接                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  架构特点:                                                        │
│                                                                 │
│  | 特性        | Doris       | ClickHouse | StarRocks |        │
│  |------------|-------------|------------|-----------|        │
│  | 存储模型    | 行+列       | 列式       | 列式      |        │
│  | 更新模型    | 唯一键      | MergeTree  | 主键      |        |
│  | 复制        | 多副本      | 可配置     | 多副本    |        |
│  | SQL 兼容    | MySQL       | 有限       | MySQL     |        |
│  | 实时写入    | 支持        | 支持       | 支持      |        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 核心术语

```sql
-- Doris 核心术语

-- 1. FE (Frontend) - 前端
-- 负责 SQL 解析、查询规划、元数据管理

-- 2. BE (Backend) - 后端
-- 负责数据存储、查询执行

-- 3. Tablet - 数据分片
-- 表数据的最小分片单元
-- 分布在多个 BE 节点

-- 4. Partition - 分区
-- 逻辑分区，按分区键划分
-- 支持 Range/List 分区

-- 5. Rollup - 物化索引
-- 表的物化视图，加速查询

-- 6. Broker - 外部数据导入
-- 负责从 HDFS/S3 导入数据

-- 7. Cluster - 集群
-- 包含多个 FE 和 BE 节点
```

---

## 存储架构

### 集群架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Doris 集群架构                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                       FE 节点 (Frontend)                         │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐                         │   │
│  │  │ Master  │  │ Follower│  │ Follower│                         │   │
│  │  │  (Leader)│  │         │  │         │                         │   │
│  │  │ - SQL解析 │  │ - 读请求 │  │ - 读请求 │                         │   │
│  │  │ - 规划   │  │ - 元数据 │  │ - 元数据 │                         │   │
│  │  │ - 调度   │  │ - 副本   │  │ - 副本   │                         │   │
│  │  └─────────┘  └─────────┘  └─────────┘                         │   │
│  │       │             │              │                             │   │
│  │       └─────────────┴──────────────┘                             │   │
│  │                    │                                             │   │
│  │          ┌─────────┴─────────┐                                  │   │
│  │          │   MySQL/JDBC     │                                  │   │
│  │          │   客户端连接     │                                  │   │
│  │          └──────────────────┘                                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                       BE 节点 (Backend)                          │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐           │   │
│  │  │  BE 1   │  │  BE 2   │  │  BE 3   │  │  BE N   │           │   │
│  │  ├─────────┤  ├─────────┤  ├─────────┤  ├─────────┤           │   │
│  │  │ Tablet  │  │ Tablet  │  │ Tablet  │  │ Tablet  │           │   │
│  │  │ (副本1) │  │ (副本1) │  │ (副本2) │  │ (副本3) │           │   │
│  │  ├─────────┤  ├─────────┤  ├─────────┤  ├─────────┤           │   │
│  │  │ Tablet  │  │ Tablet  │  │ Tablet  │  │ Tablet  │           │   │
│  │  │ (副本2) │  │ (副本2) │  │ (副本3) │  │ (副本1) │           │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘           │   │
│  │                                                                  │   │
│  │  - 数据存储     - 查询执行     - compaction    - 数据均衡       │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Tablet 存储

```
┌─────────────────────────────────────────────────────────────────┐
│                    Tablet 数据结构                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Tablet = Partition 的子集                                       │
│                                                                 │
│  表: orders (4 副本)                                             │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Partition 1 (2024-01)                                  │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │   │
│  │  │ Tablet1 │  │ Tablet2 │  │ Tablet3 │  │ Tablet4 │   │   │
│  │  │ (BE1)   │  │ (BE2)   │  │ (BE3)   │  │ (BE1)   │   │   │
│  │  │ (副本)  │  │ (副本)  │  │ (副本)  │  │ (副本)  │   │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Tablet 文件结构:                                                 │
│  /data/doris/be/                                         │
│  └── storage/                                                   │
│      └── data/                                                   │
│          └── tablet_xxx/                                         │
│              ├── 0/                                              │
│              │   ├── data_0                                      │
│              │   ├── index_0                                     │
│              │   └── delta_0                                     │
│              ├── 1/                                              │
│              └── RUN/                                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 数据模型

### 数据模型类型

```sql
-- 1. Unique 模型 (唯一键)
-- 同一唯一键只保留最新值
CREATE TABLE users (
    user_id     BIGINT      NOT NULL,
    name        VARCHAR(50),
    email       VARCHAR(100),
    age         INT,
    create_time DATETIME    DEFAULT CURRENT_TIMESTAMP
) UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true"
);

-- 2. Aggregate 模型 (聚合模型)
-- 相同 Key 自动聚合
CREATE TABLE sales (
    order_id    BIGINT      NOT NULL,
    product_id  BIGINT      NOT NULL,
    amount      SUM         DEFAULT 0,
    quantity    SUM         DEFAULT 0,
    create_time DATETIME    DEFAULT CURRENT_TIMESTAMP
) AGGREGATE KEY(order_id, product_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "3"
);

-- 3. Duplicate 模型 (明细模型)
-- 保留所有数据
CREATE TABLE events (
    event_id    BIGINT      NOT NULL,
    event_time  DATETIME,
    event_type  VARCHAR(50),
    user_id     BIGINT,
    properties  TEXT
) DUPLICATE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3"
);

-- 聚合类型:
-- SUM - 求和
-- MAX - 最大值
-- MIN - 最小值
-- REPLACE - 替换
-- REPLACE_IF_NOT_NULL - 非空替换
-- HLL_UNION - HyperLogLog 聚合
-- BITMAP_UNION - 位图聚合
```

### 分区设计

```sql
-- 1. Range 分区
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

-- 2. List 分区
CREATE TABLE users (
    user_id   BIGINT NOT NULL,
    name      VARCHAR(50),
    region    VARCHAR(20)
) UNIQUE KEY(user_id)
PARTITION BY LIST(region) (
    PARTITION p_north VALUES IN ('北京', '天津', '河北'),
    PARTITION p_south VALUES IN ('上海', '江苏', '浙江'),
    PARTITION p_other VALUES IN (DEFAULT)
)
DISTRIBUTED BY HASH(user_id) BUCKETS 4
PROPERTIES ("replication_num" = "3");

-- 3. 分区操作
-- 查看分区
SHOW PARTITIONS FROM orders;

-- 添加分区
ALTER TABLE orders ADD PARTITION p202404
VALUES LESS THAN ('2024-05-01');

-- 删除分区
ALTER TABLE orders DROP PARTITION p202401;

-- 恢复分区
ALTER TABLE orders RECOVER PARTITION p202401;

-- 分区 Split
ALTER TABLE orders SPLIT PARTITION pmax
INTO (
    PARTITION p202404 VALUES LESS THAN ('2024-05-01'),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);
```

---

## 查询引擎

### 查询流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      Doris 查询流程                                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. Client 连接                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  MySQL Client / JDBC / Python                                   │   │
│  │  - 发送 SQL                                                     │   │
│  │  - 接收结果                                                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  2. FE 解析和规划                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  SQL Parser → Analyzer → Optimizer → Scheduler                   │   │
│  │  - 语法解析                                                     │   │
│  │  - 语义分析                                                     │   │
│  │  - 查询优化 (CBO/RBO)                                           │   │
│  │  - 并行计划生成                                                 │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  3. 分布式执行                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐          │   │
│  │  │  BE 1   │  │  BE 2   │  │  BE 3   │  │  BE 4   │          │   │
│  │  │ Fragment│  │ Fragment│  │ Fragment│  │ Fragment│          │   │
│  │  │ Exec    │  │ Exec    │  │ Exec    │  │ Exec    │          │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘          │   │
│  │       │              │              │              │          │   │
│  │       └──────────────┼──────────────┼──────────────┘          │   │
│  │                      ▼              ▼                          │   │
│  │              ┌──────────────┐  ┌──────────────┐               │   │
│  │              │   Result    │  │   Result    │               │   │
│  │              │   Merge     │  │   Merge     │               │   │
│  │              └──────┬──────┘  └──────┬──────┘               │   │
│  │                     │                │                        │   │
│  │                     └────────────────┘                        │   │
│  │                              │                                 │   │
│  └──────────────────────────────┼────────────────────────────────┘   │
│                                 ▼                                      │
│  4. 结果返回                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  FE 汇总结果 → 返回客户端                                         │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 物化视图

```sql
-- 1. 创建物化视图
CREATE MATERIALIZED VIEW mv_sales_category
BUILD IMMEDIATE REFRESH ON COMMIT
AS SELECT
    product_category,
    toDate(order_time) AS order_date,
    SUM(amount) AS total_amount,
    COUNT(*) AS order_count
FROM orders
GROUP BY product_category, order_date;

-- 2. 查看物化视图
SHOW MATERIALIZED VIEWS FROM database;

-- 3. 物化视图管理
-- 刷新物化视图
REFRESH MATERIALIZED VIEW mv_sales_category;

-- 删除物化视图
DROP MATERIALIZED VIEW IF EXISTS mv_sales_category;

-- 4. 使用物化视图
-- Doris 自动选择最优物化视图
SELECT
    product_category,
    order_date,
    total_amount
FROM mv_sales_category
WHERE order_date = '2024-01-15';

-- 5. 查看物化视图使用情况
SELECT * FROM information_schema.materialized_views
WHERE table_name = 'orders';
```

---

## 高可用机制

### FE 高可用

```sql
-- FE 部署架构
-- Master (Leader) + Follower (Observer)

-- 配置多 FE
-- fe.conf
priority_networks = 192.168.1.0/24
edit_log_port = 9010

-- 查看 FE 状态
SHOW FRONTENDS;

-- 添加 FE 节点
ALTER SYSTEM ADD FOLLOWER "fe_host:9010";
ALTER SYSTEM ADD OBSERVER "fe_host:9010";

-- FE 故障转移
-- 当 Master 故障时，Follower 自动选举新 Master
-- 使用 BDB JE 完成 Leader 选举
```

### BE 高可用

```sql
-- BE 高可用配置

-- 1. 副本机制
-- Tablet 多副本存储
-- 默认 3 副本，可配置

-- 2. 副本均衡
-- 自动负载均衡
-- 手动均衡命令
ADMIN SHOW REPLICA DISTRIBUTION;
ADMIN BALANCE TABLE;

-- 3. 副本修复
-- 自动检测损坏副本
-- 自动从其他副本拉取数据修复
SHOW PROC '/cluster_health/tablet_health';

-- 4. BE 节点管理
-- 添加 BE
ALTER SYSTEM ADD BACKEND "be_host:9050";

-- 下线 BE
ALTER SYSTEM DECOMMISSION BACKEND "be_host:9050";

-- 恢复 BE
ALTER SYSTEM DROP BACKEND "be_host:9050";

-- 查看 BE 状态
SHOW BACKENDS;
```

---

## 扩缩容机制

### 水平扩展

```bash
#!/bin/bash
# scale_out.sh - 水平扩展脚本

# 1. 添加 BE 节点
mysql -h fe_host -P 9030 -u root -p
ALTER SYSTEM ADD BACKEND "new_be_host:9050";

# 2. 查看集群状态
mysql> SHOW PROC '/cluster_health/tablet_health';
mysql> SHOW PROC '/backends';

# 3. 触发均衡
mysql> ADMIN SHOW REPLICA DISTRIBUTION;
mysql> ADMIN BALANCE TABLE;

# 4. 监控扩容进度
mysql> SHOW PROC '/cluster_health/balance';
```

### 缩容操作

```bash
#!/bin/bash
# scale_in.sh - 缩容脚本

# 1. 下线 BE 节点
mysql -h fe_host -P 9030 -u root -p
ALTER SYSTEM DECOMMISSION BACKEND "old_be_host:9050";

# 2. 等待数据迁移完成
-- 检查 tablet 迁移状态
SELECT * FROM information_schema.backend_active;

# 3. 确认下线完成
-- 查看副本分布
ADMIN SHOW REPLICA DISTRIBUTION;

# 4. 移除节点 (可选)
ALTER SYSTEM DROP BACKEND "old_be_host:9050";
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [02-data-model.md](02-data-model.md) | 数据模型详解 |
| [03-data-operations.md](03-data-operations.md) | 数据操作指南 |
| [04-sql-reference.md](04-sql-reference.md) | SQL 参考 |
| [05-operations.md](05-operations.md) | 运维指南 |
| [README.md](README.md) | 索引文档 |
