# ClickHouse 数据操作指南

## 目录

- [数据插入](#数据插入)
- [数据查询](#数据查询)
- [数据更新删除](#数据更新删除)
- [物化视图](#物化视图)
- [字典](#字典)
- [导入导出](#导入导出)

---

## 数据插入

### INSERT 语句

```sql
-- 1. 单条插入
INSERT INTO events (event_time, event_type, user_id, data)
VALUES (now(), 'click', 'user123', '{"page": "/home"}');

-- 2. 多条插入
INSERT INTO events VALUES
    ('2024-01-15 10:00:00', 'view', 'user1', '{"page": "/a"}'),
    ('2024-01-15 10:01:00', 'click', 'user2', '{"page": "/b"}'),
    ('2024-01-15 10:02:00', 'purchase', 'user3', '{"amount": 99.9}');

-- 3. 从查询插入
INSERT INTO events_aggregated
SELECT
    toDate(event_time) AS date,
    event_type,
    count() AS cnt,
    uniqExact(user_id) AS unique_users
FROM events
GROUP BY date, event_type;

-- 4. 指定列插入
INSERT INTO events (event_time, event_type, user_id)
SELECT
    event_time,
    event_type,
    user_id
FROM staging_events;

-- 5. 大批量插入优化
SET max_insert_block_size = 1048576;
INSERT INTO large_table SELECT * FROM external_table;

-- 6. 异步插入
SET async_insert = 1;
INSERT INTO events VALUES ('...');  -- 异步写入 Buffer

-- 7. Buffer 表插入
CREATE TABLE events_buffer AS events
ENGINE = Buffer(default, events, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

INSERT INTO events_buffer VALUES ('...');  -- 先写入 Buffer
```

### 批量导入

```sql
-- 1. 从文件导入
-- TSV 格式
INSERT INTO events
FORMAT TSVWithNames
FROM INFILE 'events.tsv';

-- CSV 格式
INSERT INTO events
FORMAT CSVWithNames
FROM INFILE 'events.csv';

-- JSON 格式
INSERT INTO events
FORMAT JSONEachRow
FROM INFILE 'events.json';

-- 2. 使用 clickhouse-client
clickhouse-client --query "INSERT INTO events FORMAT JSONEachRow" \
    --max_block_size=10000 < events.json

-- 3. 使用 curl
curl -X POST 'http://clickhouse:8123/?user=default&password=pass' \
    --data-binary "INSERT INTO events FORMAT JSONEachRow $(cat events.json)"

-- 4. 分区并行导入
-- 将大文件按分区键拆分
-- 并行导入到不同分区

-- 5. 分布式导入
INSERT INTO distributed_events
SELECT * FROM local_events;
```

---

## 数据查询

### 基础查询

```sql
-- 1. SELECT 基础
SELECT * FROM events;
SELECT event_time, event_type, user_id FROM events;
SELECT count() FROM events;
SELECT count(DISTINCT user_id) FROM events;

-- 2. WHERE 条件
SELECT * FROM events
WHERE event_time >= '2024-01-15'
  AND event_type = 'purchase'
  AND user_id LIKE 'user%';

-- 3. GROUP BY
SELECT
    event_type,
    toDate(event_time) AS date,
    count() AS cnt,
    sum(amount) AS total_amount
FROM events
GROUP BY event_type, date;

-- 4. ORDER BY
SELECT *
FROM events
ORDER BY event_time DESC
LIMIT 100;

-- 5. HAVING
SELECT
    user_id,
    count() AS cnt
FROM events
GROUP BY user_id
HAVING cnt > 100;

-- 6. 子查询
SELECT *
FROM (
    SELECT user_id, count() AS cnt
    FROM events
    GROUP BY user_id
) WHERE cnt > 1000;
```

### 高级查询

```sql
-- 1. JOIN 操作
SELECT
    e.event_id,
    e.user_id,
    u.name,
    u.email
FROM events e
ANY LEFT JOIN users u ON e.user_id = u.id;

-- JOIN 类型
-- ALL - 笛卡尔积后去重
-- ANY - 只取第一条匹配
-- ASOF - 最接近时间匹配

-- 2. 窗口函数
SELECT
    user_id,
    event_time,
    amount,
    sum(amount) OVER (PARTITION BY user_id
                      ORDER BY event_time
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM purchases;

-- 3. WITH 子句
WITH (
    SELECT sum(amount)
    FROM purchases
    WHERE toDate(event_time) = today()
) AS today_total
SELECT
    user_id,
    amount / today_total * 100 AS percent_of_total
FROM purchases
WHERE toDate(event_time) = today();

-- 4. CTE (Common Table Expression)
WITH user_stats AS (
    SELECT
        user_id,
        count() AS event_count,
        min(event_time) AS first_event,
        max(event_time) AS last_event
    FROM events
    GROUP BY user_id
)
SELECT *
FROM user_stats
WHERE event_count > 100;

-- 5. ARRAY 操作
SELECT
    user_id,
    groupArray(event_type) AS events,
    arraySort(x -> x, events) AS sorted_events
FROM events
GROUP BY user_id;

-- 6. NULL 处理
SELECT
    user_id,
    coalesce(email, 'unknown') AS email,
    ifNull(email, 'unknown') AS email2,
    nullIf('', 'default') AS null_value
FROM users;
```

### 时间查询

```sql
-- 1. 时间函数
SELECT
    now() AS current_time,
    today() AS current_date,
    toDateTime('2024-01-15 10:00:00') AS dt,
    toDate(dt) AS d,
    toYear(dt) AS year,
    toMonth(dt) AS month,
    toDayOfMonth(dt) AS day,
    toHour(dt) AS hour,
    toMinute(dt) AS minute,
    toSecond(dt) AS second,
    toDayOfWeek(dt) AS weekday,
    toDayOfYear(dt) AS yearday,
    toWeek(dt) AS week,
    toQuarter(dt) AS quarter;

-- 2. 时间范围查询
SELECT *
FROM events
WHERE event_time >= toDateTime('2024-01-15 00:00:00')
  AND event_time < toDateTime('2024-01-16 00:00:00');

-- 本周数据
SELECT *
FROM events
WHERE event_time >= toStartOfWeek(today())
  AND event_time < toStartOfWeek(today()) + 7;

-- 3. 时区转换
SELECT
    event_time,
    event_time AT TIME ZONE 'Asia/Shanghai' AS local_time,
    event_time AT TIME ZONE 'UTC' AS utc_time
FROM events;

-- 4. 时间差
SELECT
    user_id,
    min(event_time) AS first_event,
    max(event_time) AS last_event,
    dateDiff('hour', first_event, last_event) AS hours_active,
    dateDiff('minute', first_event, last_event) AS minutes_active
FROM events
GROUP BY user_id;
```

---

## 数据更新删除

### UPDATE/DELETE

```sql
-- 1. ALTER UPDATE
ALTER TABLE events
UPDATE event_type = 'page_view'
WHERE event_type = 'view';

-- 2. ALTER DELETE
ALTER TABLE events
DELETE
WHERE event_time < toDateTime('2023-01-01');

-- 3. 批量更新
ALTER TABLE users
UPDATE
    last_login = now(),
    login_count = login_count + 1
WHERE user_id IN (
    SELECT user_id FROM active_users
);

-- 4. 突变 (Mutations)
-- 后台异步执行
SELECT
    mutation_id,
    database,
    table,
    command,
    create_time,
    apply_time,
    is_done
FROM system.mutations
WHERE database = 'default' AND table = 'events';

-- 5. 清理突变
-- 突变完成后手动清理
OPTIMIZE TABLE events FINAL;
```

### 高效删除

```sql
-- 1. 按分区删除 (推荐)
-- 删除整个分区，高效
ALTER TABLE events
DROP PARTITION TO '202301';

-- 2. 按时间删除
-- 使用 TTL 自动清理
CREATE TABLE events_ttl (
    event_time DateTime,
    data       String
) ENGINE = MergeTree()
ORDER BY event_time
TTL event_time + INTERVAL 3 MONTH;

-- 手动触发 TTL
ALTER TABLE events_ttl
MODIFY TTL event_time + INTERVAL 3 MONTH;

-- 3. 标记删除
-- 添加 deleted 标记列
ALTER TABLE events
ADD COLUMN deleted UInt8 DEFAULT 0;

-- 标记删除
ALTER TABLE events
UPDATE deleted = 1
WHERE event_time < '2023-01-01';

-- 查询时过滤
SELECT *
FROM events
WHERE deleted = 0;

-- 4. 重写表
-- 新建表，复制保留数据，删除旧表
CREATE TABLE events_new AS events
ENGINE = MergeTree()
ORDER BY event_time;

INSERT INTO events_new
SELECT * FROM events
WHERE event_time >= '2023-01-01';

RENAME TABLE events TO events_old, events_new TO events;
DROP TABLE events_old;
```

---

## 物化视图

### 创建物化视图

```sql
-- 1. 基础物化视图
CREATE MATERIALIZED VIEW hourly_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, event_type)
AS SELECT
    toStartOfHour(event_time) AS hour,
    event_type,
    count() AS cnt,
    uniqExact(user_id) AS unique_users,
    sum(amount) AS total_amount
FROM events
GROUP BY hour, event_type;

-- 2. 带条件的物化视图
CREATE MATERIALIZED VIEW purchase_stats
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(purchase_time)
ORDER BY (purchase_time, product_category)
AS SELECT
    toStartOfHour(purchase_time) AS purchase_time,
    product_category,
    count() AS cnt,
    sum(amount) AS total_amount
FROM purchases
WHERE event_type = 'purchase'
GROUP BY purchase_time, product_category;

-- 3. 聚合物化视图
CREATE MATERIALIZED VIEW user_metrics
ENGINE = AggregatingMergeTree()
ORDER BY (user_id, metric_date)
AS SELECT
    user_id,
    toDate(event_time) AS metric_date,
    countState() AS event_count,
    sumState(amount) AS amount_sum,
    uniqExactState(user_id) AS unique_visitors
FROM events
GROUP BY user_id, metric_date;

-- 4. 嵌套物化视图
CREATE MATERIALIZED VIEW daily_sales
ENGINE = SummingMergeTree()
ORDER BY (date, category)
AS SELECT
    toDate(order_time) AS date,
    category,
    count() AS order_count,
    sum(amount) AS total_amount
FROM orders
GROUP BY date, category;

CREATE MATERIALIZED VIEW monthly_sales
ENGINE = SummingMergeTree()
ORDER BY (year, month)
AS SELECT
    toYear(date) AS year,
    toMonth(date) AS month,
    category,
    sum(order_count) AS total_orders,
    sum(total_amount) AS total_revenue
FROM daily_sales
GROUP BY year, month, category;
```

### 物化视图管理

```sql
-- 查看物化视图
SELECT
    database,
    name,
    engine,
    query
FROM system.tables
WHERE engine = 'MaterializedView';

-- 刷新物化视图
-- 手动刷新
ALTER MATERIALIZED VIEW hourly_stats
REFRESH;

-- 自动刷新 (定时)
CREATE MATERIALIZED VIEW hourly_stats
ENGINE = SummingMergeTree()
...
TTL toStartOfHour(now()) + INTERVAL 1 HOUR;

-- 删除物化视图
DROP MATERIALIZED VIEW IF EXISTS hourly_stats;

-- 修改物化视图
ALTER MATERIALIZED VIEW hourly_stats
MODIFY QUERY
AS SELECT ...;

-- 重建物化视图
DETACH MATERIALIZED VIEW hourly_stats;
DROP TABLE IF EXISTS hourly_stats_view;
CREATE MATERIALIZED VIEW hourly_stats ...;
ATTACH MATERIALIZED VIEW hourly_stats FROM hourly_stats_path;
```

---

## 字典

### 字典配置

```sql
-- 1. 内部字典
CREATE DICTIONARY users_dict (
    user_id UInt64,
    name    String,
    email   String,
    status  Enum8('active'=1, 'inactive'=2, 'banned'=3)
)
PRIMARY KEY user_id
SOURCE(CLICKHOUSE(TABLE 'users'))
LIFETIME(MIN 3600, MAX 7200);

-- 2. MySQL 字典
CREATE DICTIONARY products_dict (
    product_id UInt64,
    name       String,
    category   String,
    price      Decimal(10, 2)
)
PRIMARY KEY product_id
SOURCE(MYSQL(
    host='mysql_host'
    port=3306
    user='readonly'
    password='pass'
    db='ecommerce'
    table='products'
))
LIFETIME(MIN 300, MAX 600);

-- 3. HTTP 字典
CREATE DICTIONARY exchange_rates (
    currency String,
    rate     Float64,
    updated  DateTime
)
PRIMARY KEY currency
SOURCE(HTTP(
    url='https://api.exchangerate.host/latest?base=USD'
    format='JSONEachRow'
))
LIFETIME(MIN 3600, MAX 7200);

-- 4. 自动更新字典
-- 在配置文件中设置
<dictionaries>
    <dictionary>
        <name>users_dict</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>9000</port>
                <user>default</user>
                <password></password>
                <db>default</db>
                <table>users</table>
            </clickhouse>
        </source>
        <lifetime>
            <min>300</min>
            <max>600</max>
        </lifetime>
    </dictionary>
</dictionaries>
```

### 使用字典

```sql
-- 1. 在查询中使用字典
SELECT
    e.event_id,
    e.user_id,
    dictGet('users_dict', 'name', toUInt64(e.user_id)) AS user_name,
    dictGet('users_dict', 'status', toUInt64(e.user_id)) AS user_status
FROM events e;

-- 2. 字典 JOIN
SELECT
    e.*,
    u.name,
    u.email
FROM events e
ANY LEFT JOIN users_dict u ON toUInt64(e.user_id) = u.user_id;

-- 3. 字典函数
dictGet('dict_name', 'column', key)
dictGetOrDefault('dict_name', 'column', key, default)
dictHas('dict_name', key)
dictGetHierarchy('dict_name', key)
dictIsIn('dict_name', key, ancestor_key)

-- 4. 查看字典状态
SELECT
    name,
    status,
    last_exception,
    loading_start_time,
    loading_duration,
    bytes,
    elements
FROM system.dictionaries;

-- 5. 手动加载字典
SYSTEM RELOAD DICTIONARY users_dict;
SYSTEM RELOAD DICTIONARIES;
```

---

## 导入导出

### 数据导出

```sql
-- 1. 导出到文件
-- TSV 格式
INSERT INTO OUTFILE '/tmp/events.tsv'
FORMAT TSVWithNames
SELECT * FROM events
WHERE event_time >= '2024-01-01';

-- CSV 格式
INSERT INTO OUTFILE '/tmp/events.csv'
FORMAT CSVWithNames
SELECT * FROM events;

-- JSON 格式
INSERT INTO OUTFILE '/tmp/events.json'
FORMAT JSONEachRow
SELECT * FROM events
LIMIT 1000;

-- 2. 导出压缩文件
INSERT INTO OUTFILE '/tmp/events.tsv.gz'
FORMAT TSVWithNames
GZIP
SELECT * FROM events;

INSERT INTO OUTFILE '/tmp/events.tsv.zst'
FORMAT TSVWithNames
ZSTD
SELECT * FROM events;

-- 3. 导出到 HDFS
INSERT INTO OUTFILE 'hdfs://namenode:8020/export/events.tsv'
FORMAT TSVWithNames
SELECT * FROM events;

-- 4. 使用 clickhouse-client 导出
clickhouse-client --query "SELECT * FROM events FORMAT CSVWithNames" \
    > events.csv

clickhouse-client --query "SELECT * FROM events FORMAT JSONEachRow" \
    | gzip > events.json.gz

-- 5. 导出到其他数据库
-- 使用 JDBC 或 MySQL 引擎
```

### 数据导入工具

```bash
#!/bin/bash
# import_data.sh - 数据导入脚本

# 1. 使用 clickhouse-client
clickhouse-client \
    --host clickhouse-01 \
    --port 9000 \
    --user default \
    --password pass \
    --database analytics \
    --queries_file import.sql \
    --format_csv_delimiter ',' \
    --max_insert_block_size 100000 \
    --input_format_parallel_parsing=1 \
    --insert_distributed_timeout=0

# 2. 使用 clickhouse-import
clickhouse-import \
    --host clickhouse-01 \
    --port 9000 \
    --database analytics \
    --table events \
    --format JSONEachRow \
    --threads 4 \
    events.json

# 3. 并行导入
for i in {1..10}; do
    clickhouse-client --query "INSERT INTO events FORMAT JSONEachRow" \
        < part_$i.json &
done
wait

# 4. 使用 DataX (阿里)
# 参考 DataX ClickHouse 插件配置
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-merge-tree.md](02-merge-tree.md) | MergeTree 表引擎 |
| [04-optimization.md](04-optimization.md) | 查询优化指南 |
| [05-sql-reference.md](05-sql-reference.md) | SQL 参考 |
| [README.md](README.md) | 索引文档 |
