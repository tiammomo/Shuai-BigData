# ClickHouse 查询优化指南

## 目录

- [查询优化原则](#查询优化原则)
- [索引优化](#索引优化)
- [查询改写](#查询改写)
- [资源管理](#资源管理)
- [监控诊断](#监控诊断)

---

## 查询优化原则

```sql
-- 1. 只查询必要列
-- 不好的写法
SELECT * FROM large_table WHERE date = '2024-01-15';

-- 好的写法
SELECT
    event_id,
    event_time,
    user_id,
    event_type
FROM large_table
WHERE date = '2024-01-15';

-- 2. 使用分区裁剪
-- 按分区键过滤
SELECT *
FROM events
WHERE toYYYYMM(event_time) = 202401;

-- 3. 尽早过滤
-- 不好的写法
SELECT *
FROM (
    SELECT * FROM events WHERE event_type = 'purchase'
) e
JOIN users u ON e.user_id = u.id
WHERE u.status = 'active';

-- 好的写法
SELECT e.*, u.name
FROM events e
ANY LEFT JOIN users u ON e.user_id = u.id
WHERE e.event_type = 'purchase'
  AND u.status = 'active';

-- 4. 使用 PREWHERE
-- 提前过滤列
SELECT count()
FROM visits
PREWHERE toStartOfMonth(visit_date) = '2024-01-01'
WHERE user_id = 12345;

-- 5. 避免 SELECT DISTINCT
-- 改用 GROUP BY
SELECT DISTINCT user_id FROM events;

-- 更好的写法
SELECT user_id FROM events GROUP BY user_id;
```

---

## 索引优化

```sql
-- 1. 优化 ORDER BY
-- 低基数放前面
CREATE TABLE orders (
    order_id    UInt64,
    customer_id UInt64,
    order_time  DateTime,
    amount      Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (customer_id, order_time, order_id);

-- 2. 添加跳数索引
ALTER TABLE web_logs
ADD INDEX page_idx (page_url) TYPE tokenbf_tokenizer(32768, 3, 0) GRANULARITY 4;

ALTER TABLE web_logs
ADD INDEX status_idx (status_code) TYPE set(100) GRANULARITY 4;

-- 3. 选择合适的分区粒度
-- 数据量大时减小粒度
CREATE TABLE large_events (
    event_time DateTime,
    data       String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time)
SETTINGS index_granularity = 4096;  -- 减小粒度

-- 4. 物化列
-- 用于频繁计算的表达式
CREATE TABLE sales (
    order_time   DateTime,
    amount       Decimal(10, 2)
) ENGINE = MergeTree()
ORDER BY (order_time);

ALTER TABLE sales
ADD MATERIALIZED COLUMN month AS toMonth(order_time);

SELECT
    month,
    sum(amount) AS total
FROM sales
GROUP BY month;
```

---

## 查询改写

```sql
-- 1. 改写 IN 为 JOIN
-- 不好的写法
SELECT *
FROM events
WHERE user_id IN (SELECT id FROM active_users);

-- 好的写法
SELECT e.*
FROM events e
ANY LEFT JOIN active_users au ON e.user_id = au.id
WHERE au.id IS NOT NULL;

-- 2. 改写子查询为 WITH
-- 不好的写法
SELECT *
FROM (
    SELECT date, sum(amount) AS daily
    FROM orders
    GROUP BY date
) t1
WHERE daily > (
    SELECT avg(daily)
    FROM (
        SELECT date, sum(amount) AS daily
        FROM orders
        GROUP BY date
    ) t2
);

-- 好的写法
WITH daily_sales AS (
    SELECT toDate(order_time) AS date, sum(amount) AS daily
    FROM orders
    GROUP BY date
)
SELECT *
FROM daily_sales
WHERE daily > (
    SELECT avg(daily) FROM daily_sales
);

-- 3. 使用物化视图
-- 预先计算聚合
CREATE MATERIALIZED VIEW daily_summary
ENGINE = SummingMergeTree()
ORDER BY (date)
AS SELECT
    toDate(order_time) AS date,
    count() AS order_count,
    sum(amount) AS total_amount
FROM orders
GROUP BY date;

-- 查询物化视图
SELECT * FROM daily_summary WHERE date = today();

-- 4. 使用 APPROX
-- 近似计算加速
SELECT
    count() AS exact_count,
    countExact() AS exact_count2,
    uniq(user_id) AS approx_unique,
    uniqExact(user_id) AS exact_unique
FROM events;

SELECT quantile(0.5)(amount) FROM orders;
SELECT quantileExact(0.5)(amount) FROM orders;
```

---

## 资源管理

```sql
-- 1. 设置查询限制
SET max_execution_time = 60;
SET max_rows_to_read = 1000000000;
SET max_bytes_to_read = 10737418240;
SET max_rows_to_group_by = 1000000;
SET max_memory_usage = 10737418240;
SET max_result_rows = 1000000;

-- 2. 查询优先级
SET priority = 0;  -- 0: normal, 1: low, 2: high

-- 3. 使用资源池
-- 在配置文件中
<profiles>
    <default>
        <max_memory_usage>10000000000</max_memory_usage>
        <max_execution_time>60</max_execution_time>
    </default>
    <interactive>
        <max_memory_usage>5000000000</max_memory_usage>
        <max_execution_time>10</max_execution_time>
    </interactive>
    <background>
        <max_memory_usage>20000000000</max_memory_usage>
        <max_execution_time>3600</max_execution_time>
    </background>
</profiles>

-- 4. 限制并发查询
SELECT count()
FROM system.processes;

-- 取消长时间运行查询
KILL QUERY WHERE query_id = 'query_id';
```

---

## 监控诊断

```sql
-- 1. 查询日志
-- 慢查询
SELECT
    query_start_time,
    query_duration_ms,
    rows_read,
    bytes_read,
    query_kind,
    query
FROM system.query_log
WHERE event_date = today()
  AND query_duration_ms > 5000
ORDER BY query_duration_ms DESC
LIMIT 100;

-- 2. 查询指标
SELECT
    profile_events['RowsRead'] AS rows_read,
    profile_events['BytesRead'] AS bytes_read,
    profile_events['ElapsedMicroseconds'] / 1000000 AS elapsed,
    profile_events['SoftQueries'] AS soft_queries
FROM system.query_log
WHERE query_id = 'current_query_id';

-- 3. 表统计
SELECT
    name,
    rows,
    marks,
    bytes,
    bytes_uncompressed,
    primary_key_bytes_in_memory
FROM system.parts
WHERE database = 'default' AND table = 'events';

-- 4. 索引使用
SELECT
    name,
    type,
    is_primary,
    is_unique,
    skipped_by_index_granularity,
    used_by_merge_tree_optimizer,
    merge_tree_optimizer_rejected_parts,
    merge_tree_optimizer_accepted_parts
FROM system.parts_where_index_not_used
WHERE database = 'default' AND table = 'events';

-- 5. 线程分析
SELECT
    thread_name,
    current_memory_usage,
    peak_memory_usage,
    query_thread_count
FROM system.query_thread_log;

-- 6. EXPLAIN
EXPLAIN SELECT
    event_type,
    count() AS cnt
FROM events
GROUP BY event_type;

EXPLAIN SYNTAX SELECT * FROM events WHERE event_time > today();

EXPLAIN PIPELINE SELECT count() FROM events;
```

---

## 最佳实践

### 表设计最佳实践

```sql
-- 1. 选择合适的数据类型
-- 使用最小满足需求的类型
CREATE TABLE optimized_types (
    -- 使用整数类型
    flag UInt8,              -- 替代 Boolean
    count UInt64,            -- 大计数

    -- 使用日期时间类型
    created_at DateTime,     -- 日期时间
    created_date Date,       -- 只有日期

    -- 使用字符串类型
    name String,             -- 变长字符串
    status Enum8('active'=1, 'inactive'=2),  -- 枚举

    -- 使用数组类型
    tags Array(String),      -- 标签数组

    -- 使用嵌套类型
    coordinates Nested(
        x Float32,
        y Float32
    )
) ENGINE = MergeTree()
ORDER BY (created_date, flag);

-- 2. 合适的主键选择
-- 选择查询过滤常用的列
CREATE TABLE events (
    event_id UInt64,
    event_time DateTime,
    user_id UInt64,
    event_type String,
    platform String,
    country String
) ENGINE = MergeTree()
ORDER BY (event_type, platform, event_time)
PARTITION BY toYYYYMM(event_time);

-- 3. 压缩设置
-- LZ4: 默认，快速
-- ZSTD: 高压缩比，适合历史数据
-- Delta: 适合自增序列
-- Gorilla: 适合浮点数

-- 4. 合适的 Granularity
-- 默认 8192，适合大多数场景
-- 数据量大时可调大
CREATE TABLE large_events (
    event_id UInt64,
    event_time DateTime,
    data String
) ENGINE = MergeTree()
ORDER BY (event_id)
SETTINGS index_granularity = 16384;
```

### 查询模式最佳实践

```sql
-- 1. 使用采样查询
SELECT
    event_type,
    count() * 10 AS estimated_total  -- 假设采样 10%
FROM events
SAMPLE 0.1  -- 采样 10%
GROUP BY event_type;

-- 2. 使用 FINAL
-- 保证去重和合并
SELECT
    user_id,
    max(session_start)
FROM user_sessions
FINAL
GROUP BY user_id;

-- 3. 限制结果
-- 使用 LIMIT 避免意外的大结果
SELECT event_type, count()
FROM events
WHERE event_time >= now() - INTERVAL 1 DAY
GROUP BY event_type
LIMIT 100;

-- 4. 使用 HAVING 替代 WHERE
-- HAVING 在聚合后过滤
SELECT
    event_type,
    count() AS cnt
FROM events
GROUP BY event_type
HAVING cnt > 1000;
```

### 写入最佳实践

```java
// 1. 批量写入
public void batchInsert(Connection conn, List<Event> events) throws SQLException {
    String sql = "INSERT INTO events VALUES";
    StringBuilder sb = new StringBuilder(sql);

    int batchSize = 10000;
    for (int i = 0; i < events.size(); i++) {
        Event e = events.get(i);
        if (i > 0) sb.append(",");
        sb.append("(")
          .append(e.getEventId()).append(",")
          .append("'").append(e.getEventTime()).append("',")
          .append(e.getUserId()).append(",")
          .append("'").append(e.getEventType()).append("',")
          .append("'").append(e.getPlatform()).append("'")
          .append(")");

        if ((i + 1) % batchSize == 0) {
            conn.createStatement().execute(sb.toString());
            sb.setLength(0);
            sb.append(sql);
        }
    }

    if (sb.length() > sql.length()) {
        conn.createStatement().execute(sb.toString());
    }
}

// 2. 异步插入
String asyncSql =
    "INSERT INTO events ASYNC " +
    "SELECT * FROM s3('s3://bucket/events/*.parquet')";

// 3. 使用 Buffer 表
String bufferSql =
    "CREATE TABLE events_buffer AS events " +
    "ENGINE = Buffer(" +
    "  default, events, " +
    "  16, 10, 100, 10000, 1000000, 10000000, 60000000" +
    ")";
```

### 监控和维护

```java
// 1. 查看查询日志
String queryLog =
    "SELECT " +
    "  query_start_time, " +
    "  query_duration_ms, " +
    "  read_rows, " +
    "  written_rows, " +
    "  memory_usage, " +
    "  query " +
    "FROM system.query_log " +
    "WHERE event_time >= now() - INTERVAL 1 HOUR " +
    "ORDER BY query_duration_ms DESC " +
    "LIMIT 100";

// 2. 查看 MergeTree 状态
String mergeTreeStatus =
    "SELECT " +
    "  database, " +
    "  table, " +
    "  sum(rows) AS total_rows, " +
    "  sum(bytes_on_disk) AS total_bytes, " +
    "  max(mutation_id) AS latest_mutation " +
    "FROM system.parts " +
    "WHERE active = 1 " +
    "GROUP BY database, table";

// 3. 查看副本状态
String replicaStatus =
    "SELECT " +
    "  database, " +
    "  table, " +
    "  replica_name, " +
    "  is_leader, " +
    "  parts_to_check, " +
    "  insertions_in_queue " +
    "FROM system.replicas";

// 4. 查看磁盘使用
String diskUsage =
    "SELECT " +
    "  name, " +
    "  path, " +
    "  free_space, " +
    "  total_space, " +
    "  used_space " +
    "FROM system.disks";
```

---

## 常见问题

### Q1: 插入慢?

**现象**: 数据写入速度慢，堆积

**解决方案**:
```java
// 1. 批量插入
// 单次 INSERT 建议 100KB - 10MB
batchSize = 10000;

// 2. 检查分区键
// 避免热点分区，使用合理分区策略

// 3. 增加写入并发
int threadCount = 4;

// 4. 使用 Buffer 表缓冲写入
// 5. 禁用索引写入时同步
```

### Q2: 查询慢?

**现象**: 查询响应时间长

**解决方案**:
```sql
-- 1. 使用 EXPLAIN 查看执行计划
EXPLAIN SELECT count() FROM events;

-- 2. 检查是否走索引
-- ORDER BY 列应该是过滤常用列

-- 3. 增加 max_threads
SET max_threads = 8;

-- 4. 使用物化视图预聚合
CREATE MATERIALIZED VIEW mv_daily
ENGINE = SummingMergeTree()
AS SELECT
    toDate(event_time) AS date,
    event_type,
    count() AS cnt
FROM events
GROUP BY date, event_type;

-- 5. 使用 PREWHERE 优化
SELECT * FROM events PREWHERE event_type = 'click';
```

### Q3: 内存不足?

**现象**: 查询报 OOM 错误

**解决方案**:
```sql
-- 1. 增加 max_memory_usage
SET max_memory_usage = 34359738368;  -- 32GB

-- 2. 使用 external_sort
SET max_bytes_before_external_sort = 10737418240;

-- 3. 分批处理数据
-- 使用 LIMIT 和 OFFSET 分页

-- 4. 优化 GROUP BY
SET group_by_two_level_threshold = 1000000;
SET max_rows_to_group_by = 5000000;

-- 5. 减少 JOIN 数据量
-- 先过滤再 JOIN
```

### Q4: 数据去重问题?

**现象**: 查询结果包含重复数据

**解决方案**:
```sql
-- 1. 使用 ReplacingMergeTree
CREATE TABLE dedup_events (
    event_id UInt64,
    event_time DateTime,
    user_id UInt64
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (event_id);

-- 2. 使用 FINAL 关键字
SELECT * FROM dedup_events FINAL;

-- 3. 定期 OPTIMIZE
OPTIMIZE TABLE dedup_events FINAL;
```

### Q5: 分区管理问题?

**现象**: 分区数据不均衡

**解决方案**:
```sql
-- 1. 查看分区状态
SELECT
    partition_id,
    rows,
    bytes_on_disk
FROM system.parts
WHERE table = 'events'
ORDER BY partition_id;

-- 2. 手动合并分区
OPTIMIZE TABLE events PARTITION 202401 FINAL;

-- 3. 删除旧分区
ALTER TABLE events DROP PARTITION 202301;

-- 4. 调整分区策略
-- 按天分区适合实时分析
-- 按月分区适合历史数据
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-merge-tree.md](02-merge-tree.md) | MergeTree 表引擎 |
| [03-data-operations.md](03-data-operations.md) | 数据操作指南 |
| [05-sql-reference.md](05-sql-reference.md) | SQL 参考 |
| [README.md](README.md) | 索引文档 |
