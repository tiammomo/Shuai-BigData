# Presto/Trino 使用指南

## 目录

- [SQL 基础](#sql-基础)
- [连接器配置](#连接器配置)
- [性能优化](#性能优化)
- [高级特性](#高级特性)

---

## SQL 基础

### 数据类型

```sql
-- 数值类型
BIGINT, INTEGER, SMALLINT, TINYINT
REAL, DOUBLE

-- 字符串类型
VARCHAR, CHAR, VARBINARY

-- 时间类型
DATE, TIME, TIMESTAMP, INTERVAL

-- 其他
BOOLEAN, ARRAY, MAP, JSON
```

### 基本查询

```sql
-- 多数据源查询
SELECT * FROM mysql.mydb.users;

-- 跨数据源 JOIN
SELECT u.name, o.amount
FROM mysql.mydb.users u
JOIN hive.orders.orders o ON u.id = o.user_id
WHERE o.created_at >= '2024-01-01';

-- CTE (Common Table Expression)
WITH active_users AS (
    SELECT * FROM users WHERE status = 'active'
)
SELECT COUNT(*) FROM active_users;
```

### 聚合查询

```sql
-- GROUPING SETS
SELECT region, product, SUM(amount)
FROM sales
GROUP BY GROUPING SETS (
    (region),
    (product),
    (region, product)
);

-- CUBE
SELECT region, product, SUM(amount)
FROM sales
GROUP BY CUBE (region, product);

-- ROLLUP
SELECT region, product, SUM(amount)
FROM sales
GROUP BY ROLLUP (region, product);
```

---

## 连接器配置

### MySQL 连接器

```sql
-- 创建 MySQL Catalog
CREATE TABLE IF NOT EXISTS mysql.mydb.users (
    id BIGINT,
    name VARCHAR,
    age INTEGER
)
WITH (
    connector = 'mysql',
    connection-url = 'jdbc:mysql://localhost:3306',
    connection-user = 'root',
    connection-password = 'password'
);

-- 查询 MySQL 数据
SELECT * FROM mysql.mydb.users;
```

### Hive 连接器

```sql
-- 创建 Hive Catalog
CREATE TABLE IF NOT EXISTS hive.analytics.page_views (
    view_time TIMESTAMP,
    user_id BIGINT,
    page_url VARCHAR
)
WITH (
    connector = 'hive',
    hive.metastore = 'thrift://localhost:9083',
    hive.s3.access-key = 'xxx',
    hive.s3.secret-key = 'xxx'
);
```

### PostgreSQL 连接器

```sql
CREATE TABLE IF NOT EXISTS postgres.public.users (
    id BIGINT,
    name VARCHAR,
    email VARCHAR
)
WITH (
    connector = 'postgresql',
    connection-url = 'jdbc:postgresql://localhost:5432',
    connection-user = 'postgres',
    connection-password = 'password'
);
```

---

## 性能优化

### 查询优化

```sql
-- 1. 分区裁剪
SELECT * FROM sales
WHERE date >= '2024-01-01';  -- 利用分区

-- 2. 列裁剪
SELECT id, name FROM users;  -- 只查询需要的列

-- 3. LIMIT
SELECT * FROM large_table LIMIT 100;

-- 4. 使用近似函数
SELECT approx_distinct(user_id) FROM events;
SELECT approx_percentile(price, 0.95) FROM products;
```

### 内存配置

```sql
-- 设置会话内存
SET SESSION query_max_memory = '8GB';
SET SESSION memory_heap_headroom = '1GB';

-- 查看内存使用
SHOW SESSION LIKE 'query_max%';
```

### 并发控制

```sql
-- 设置并发度
SET SESSION task_concurrency = 4;

-- 限制输出
SET SESSION task_max_writer_count = 2;
```

---

## 高级特性

### 窗口函数

```sql
-- 排名
SELECT
    product,
    sales,
    RANK() OVER (ORDER BY sales DESC) AS rank,
    ROW_NUMBER() OVER (ORDER BY sales DESC) AS row_num
FROM products;

-- 累计
SELECT
    date,
    sales,
    SUM(sales) OVER (PARTITION BY region ORDER BY date) AS running_sales,
    AVG(sales) OVER (PARTITION BY region ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg
FROM sales;
```

### 嵌套数据类型

```sql
-- ARRAY
SELECT ARRAY[1, 2, 3] AS arr;

-- MAP
SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2]) AS map_col;

-- JSON
SELECT json_extract(properties, '$.name') AS name
FROM users;
```

### 分布式排序

```sql
-- 全局排序
SELECT * FROM large_table
ORDER BY id
LIMIT 100;

-- 分区排序
SELECT * FROM large_table
ORDER BY region, id
LIMIT 10;
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [README.md](README.md) | 索引文档 |
