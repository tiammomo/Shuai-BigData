# Doris SQL 参考

## 目录

- [DDL 语句](#ddl-语句)
- [DML 语句](#dml-语句)
- [DQL 语句](#dql-语句)
- [数据类型](#数据类型)
- [函数](#函数)
- [系统变量](#系统变量)

---

## DDL 语句

### 数据库操作

```sql
-- 1. 创建数据库
CREATE DATABASE test_db;

-- 2. 创建数据库 (带属性)
CREATE DATABASE test_db
PROPERTIES (
    "replication_num" = "3"
);

-- 3. 查看数据库
SHOW DATABASES;
SHOW CREATE DATABASE test_db;

-- 4. 使用数据库
USE test_db;

-- 5. 修改数据库
ALTER DATABASE test_db
SET PROPERTIES ("replication_num" = "3");

-- 6. 删除数据库
DROP DATABASE test_db;
DROP DATABASE IF EXISTS test_db;

-- 7. 删除数据库 (级联删除)
DROP DATABASE test_db CASCADE;
```

### 表操作

```sql
-- 1. 创建表
CREATE TABLE users (
    user_id     BIGINT      NOT NULL COMMENT "用户ID",
    username    VARCHAR(50) NOT NULL COMMENT "用户名",
    email       VARCHAR(100)        COMMENT "邮箱",
    age         INT                 COMMENT "年龄",
    status      VARCHAR(20) DEFAULT "active" COMMENT "状态",
    create_time DATETIME    DEFAULT CURRENT_TIMESTAMP COMMENT "创建时间"
) UNIQUE KEY(user_id)
COMMENT "用户表"
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3",
    "enable_unique_key_merge_on_write" = "true",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2025-01-01 00:00:00"
);

-- 2. 创建表 (从查询结果)
CREATE TABLE new_users
KEYS(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "3")
AS SELECT * FROM users WHERE create_time >= '2024-01-01';

-- 3. 创建临时表
CREATE TEMPORARY TABLE temp_users
    (user_id BIGINT, name VARCHAR(50))
DISTRIBUTED BY HASH(user_id) BUCKETS 4;

-- 4. 查看表
SHOW TABLES;
SHOW TABLES LIKE '%user%';
SHOW CREATE TABLE users;
DESC users;
DESCRIBE users;

-- 5. 修改表
-- 添加列
ALTER TABLE users ADD COLUMN phone VARCHAR(20) DEFAULT "unknown";

-- 添加多列
ALTER TABLE users
    ADD COLUMN phone VARCHAR(20) DEFAULT "unknown",
    ADD COLUMN address VARCHAR(500);

-- 删除列
ALTER TABLE users DROP COLUMN phone;

-- 修改列类型
ALTER TABLE users MODIFY COLUMN age BIGINT;

-- 重命名列
ALTER TABLE users CHANGE age user_age INT;

-- 重命名表
RENAME TABLE users TO users_new;
ALTER TABLE users RENAME users_new;

-- 修改表属性
ALTER TABLE users SET ("replication_num" = "2");

-- 6. 删除表
DROP TABLE users;
DROP TABLE IF EXISTS users;

-- 7. 清空表
TRUNCATE TABLE users;
```

### 物化视图

```sql
-- 1. 创建物化视图
CREATE MATERIALIZED VIEW mv_user_stats
BUILD IMMEDIATE REFRESH ON COMMIT
AS SELECT
    status,
    COUNT(*) as cnt
FROM users
GROUP BY status;

-- 2. 查看物化视图
SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS FROM test_db;

-- 3. 删除物化视图
DROP MATERIALIZED VIEW mv_user_stats;
DROP MATERIALIZED VIEW IF EXISTS mv_user_stats;

-- 4. 刷新物化视图
REFRESH MATERIALIZED VIEW mv_user_stats;
REFRESH MATERIALIZED VIEW mv_user_stats FORCE;
```

### 视图

```sql
-- 1. 创建视图
CREATE VIEW v_active_users AS
SELECT user_id, username, email
FROM users
WHERE status = 'active';

-- 2. 创建视图 (带条件)
CREATE VIEW v_user_orders AS
SELECT
    u.user_id,
    u.username,
    o.order_id,
    o.amount,
    o.order_time
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id;

-- 3. 查看视图
SHOW TABLES;
SHOW CREATE VIEW v_active_users;

-- 4. 修改视图
CREATE OR REPLACE VIEW v_active_users AS
SELECT user_id, username, email, phone
FROM users
WHERE status = 'active';

-- 5. 删除视图
DROP VIEW v_active_users;
DROP VIEW IF EXISTS v_active_users;
```

### 资源管理

```sql
-- 1. 创建资源
CREATE RESOURCE "spark_resource"
PROPERTIES (
    "type" = "spark",
    "spark.master" = "yarn",
    "spark.submit.deployMode" = "cluster",
    "spark.hadoop.yarn.resourcemanager.address" = "yarn:8032"
);

-- 2. 创建工作负载组
CREATE WORKLOAD GROUP IF NOT EXISTS "wg_high"
PROPERTIES (
    "cpu_share" = "400",
    "memory_limit" = "40%",
    "enable_memory_overcommit" = "true"
);

-- 3. 设置资源组
SET PROPERTY FOR 'user1' 'default_workload_group' = 'wg_high';
```

---

## DML 语句

### INSERT

```sql
-- 1. INSERT VALUES
INSERT INTO users (user_id, username, email, age)
VALUES (1001, 'Alice', 'alice@example.com', 25),
       (1002, 'Bob', 'bob@example.com', 30);

-- 2. INSERT SELECT
INSERT INTO users (user_id, username, email, age)
SELECT user_id, username, email, age
FROM staging_users
WHERE status = 'active';

-- 3. INSERT ON DUPLICATE KEY UPDATE
INSERT INTO users (user_id, username, age)
VALUES (1001, 'Alice', 25)
ON DUPLICATE KEY UPDATE
    username = 'Alice',
    age = 25;

-- 4. INSERT IGNORE
INSERT IGNORE INTO users (user_id, username, age)
VALUES (1001, 'Alice', 25);

-- 5. INSERT OVERWRITE
INSERT OVERWRITE TABLE users
SELECT * FROM staging_users;
```

### UPDATE

```sql
-- 1. UPDATE
UPDATE users
SET age = age + 1
WHERE status = 'active';

-- 2. UPDATE (多列)
UPDATE users
SET age = 26,
    status = 'vip',
    update_time = NOW()
WHERE user_id = 1001;

-- 3. UPDATE (JOIN)
UPDATE orders o
INNER JOIN users u ON o.user_id = u.user_id
SET o.status = 'processed'
WHERE u.status = 'active';
```

### DELETE

```sql
-- 1. DELETE
DELETE FROM users WHERE user_id = 1001;

-- 2. DELETE (条件)
DELETE FROM orders
WHERE order_time < DATE_SUB(NOW(), INTERVAL 1 YEAR);

-- 3. DELETE (JOIN)
DELETE o FROM orders o
INNER JOIN users u ON o.user_id = u.user_id
WHERE u.status = 'banned';
```

---

## DQL 语句

### SELECT

```sql
-- 1. 基础查询
SELECT * FROM users;
SELECT user_id, username, email FROM users;

-- 2. WHERE 条件
SELECT * FROM users WHERE age >= 18;
SELECT * FROM users WHERE status IN ('active', 'vip');
SELECT * FROM users WHERE username LIKE 'A%';
SELECT * FROM users WHERE age BETWEEN 18 AND 30;

-- 3. DISTINCT
SELECT DISTINCT status FROM users;
SELECT DISTINCT status, gender FROM users;

-- 4. ORDER BY
SELECT * FROM users ORDER BY age DESC;
SELECT * FROM users ORDER BY age DESC, create_time ASC;

-- 5. LIMIT / TOP
SELECT * FROM users LIMIT 10;
SELECT * FROM users ORDER BY age DESC LIMIT 10;

-- 6. OFFSET
SELECT * FROM users LIMIT 10 OFFSET 20;
```

### JOIN

```sql
-- 1. INNER JOIN
SELECT
    o.order_id,
    u.username,
    o.amount
FROM orders o
INNER JOIN users u ON o.user_id = u.user_id;

-- 2. LEFT JOIN
SELECT
    u.user_id,
    u.username,
    COUNT(o.order_id) as order_count
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.username;

-- 3. RIGHT JOIN
SELECT
    u.user_id,
    o.order_id,
    o.amount
FROM users u
RIGHT JOIN orders o ON u.user_id = o.user_id;

-- 4. FULL JOIN (Doris 不支持，可用 UNION 替代)
SELECT
    u.user_id,
    o.order_id,
    o.amount
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
UNION
SELECT
    u.user_id,
    o.order_id,
    o.amount
FROM users u
RIGHT JOIN orders o ON u.user_id = o.user_id
WHERE u.user_id IS NULL;

-- 5. 多表 JOIN
SELECT *
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN products p ON o.product_id = p.product_id;
```

### 聚合

```sql
-- 1. GROUP BY
SELECT status, COUNT(*) as count
FROM users
GROUP BY status;

-- 2. HAVING
SELECT
    status,
    COUNT(*) as count,
    AVG(age) as avg_age
FROM users
GROUP BY status
HAVING COUNT(*) > 10;

-- 3. GROUPING SETS
SELECT
    user_id,
    status,
    COUNT(*)
FROM users
GROUP BY GROUPING SETS (
    (user_id),
    (status),
    (user_id, status)
);

-- 4. ROLLUP
SELECT
    status,
    DATE_FORMAT(create_time, '%Y-%m') as month,
    COUNT(*)
FROM users
GROUP BY status, DATE_FORMAT(create_time, '%Y-%m')
WITH ROLLUP;

-- 5. CUBE
SELECT
    status,
    gender,
    COUNT(*)
FROM users
GROUP BY status, gender
WITH CUBE;
```

### 子查询

```sql
-- 1. IN / NOT IN
SELECT * FROM users
WHERE user_id IN (SELECT user_id FROM orders WHERE amount > 100);

-- 2. EXISTS / NOT EXISTS
SELECT * FROM users u
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.user_id
);

-- 3. FROM 子查询
SELECT * FROM (
    SELECT user_id, COUNT(*) as order_count
    FROM orders
    GROUP BY user_id
) t
WHERE t.order_count > 10;

-- 4. 标量子查询
SELECT
    *,
    (SELECT COUNT(*) FROM orders WHERE user_id = users.user_id) as order_count
FROM users;
```

---

## 数据类型

### 数值类型

```sql
-- 整数类型
TINYINT    -- 1 字节，-128 到 127
SMALLINT   -- 2 字节，-32768 到 32767
INT        -- 4 字节，-2147483648 到 2147483647
BIGINT     -- 8 字节，-9223372036854775808 到 9223372036854775807
LARGEINT   -- 16 字节

-- 浮点类型
FLOAT      -- 4 字节
DOUBLE     -- 8 字节

-- 精确类型
DECIMAL(p, s)  -- 精度 p，刻度 s
-- DECIMAL(10, 2) - 最多 10 位，其中 2 位小数

-- 布尔类型
BOOLEAN    -- TRUE/FALSE
```

### 字符串类型

```sql
-- 定长字符串
CHAR(N)    -- 固定长度 N，不足用空格填充
VARCHAR(N) -- 可变长度，最大 N
STRING     -- 可变长度，最大 10485760 (10MB)

-- TEXT 类型
TEXT       -- 最大 1GB
```

### 日期时间类型

```sql
-- 日期
DATE       -- 'YYYY-MM-DD'
DATETIME   -- 'YYYY-MM-DD HH:MM:SS'
TIMESTAMP  -- 'YYYY-MM-DD HH:MM:SS' (支持时区)

-- 时间
TIME       -- 'HH:MM:SS'

-- 复合
DATETIME(3)     -- 精确到毫秒
DATETIME(6)     -- 精确到微秒
```

### 其他类型

```sql
-- 数组
ARRAY<INT>
ARRAY<VARCHAR(100)>

-- 嵌套类型
STRUCT<name VARCHAR, age INT>

-- Map
MAP<VARCHAR, INT>

-- JSON
JSON
VARIANT (JSON)

-- 位图
BITMAP

-- HLL (HyperLogLog)
HLL
```

---

## 函数

### 聚合函数

```sql
-- COUNT
COUNT(*)        -- 所有行
COUNT(col)      -- 非空行
COUNT(DISTINCT col)  -- 去重计数

-- SUM/AVG/MIN/MAX
SUM(amount)
AVG(age)
MIN(price)
MAX(price)

-- GROUP_CONCAT
GROUP_CONCAT(name)
GROUP_CONCAT(name, ',')

-- ARRAY_AGG
ARRAY_AGG(col)

-- BITMAP
BITMAP_UNION(col)
BITMAP_COUNT(col)
TO_BITMAP(col)
```

### 字符串函数

```sql
-- 长度
LENGTH(str)
CHAR_LENGTH(str)

-- 大小写
UPPER(str)
LOWER(str)

-- 截取
LEFT(str, n)
RIGHT(str, n)
SUBSTRING(str, pos, len)
MID(str, pos, len)

-- 替换
REPLACE(str, old, new)

-- 去除空格
TRIM(str)
LTRIM(str)
RTRIM(str)

-- 连接
CONCAT(str1, str2)
CONCAT_WS(',', str1, str2)

-- 查找
INSTR(str, substr)
LOCATE(substr, str)
POSITION(substr IN str)

-- 格式转换
FORMAT(number, decimal)

-- 正则
REGEXP_REPLACE(str, regex, replace)
REGEXP_EXTRACT(str, regex, group)
```

### 日期函数

```sql
-- 获取当前时间
NOW()
CURDATE()
CURTIME()
CURRENT_TIMESTAMP()

-- 日期提取
YEAR(date)
MONTH(date)
DAY(date)
HOUR(date)
MINUTE(date)
SECOND(date)
DAYOFWEEK(date)
DAYOFMONTH(date)
DAYOFYEAR(date)
WEEK(date)
QUARTER(date)

-- 日期计算
DATE_ADD(date, INTERVAL n DAY)
DATE_SUB(date, INTERVAL n DAY)
DATEDIFF(date1, date2)
TIMESTAMPDIFF(unit, date1, date2)

-- 日期格式化
DATE_FORMAT(date, format)
STR_TO_DATE(str, format)

-- 日期转换
TO_DAYS(date)
FROM_DAYS(n)
UNIX_TIMESTAMP(date)
FROM_UNIXTIME(ts)

-- 其他
LAST_DAY(date)
```

### 数学函数

```sql
-- 绝对值
ABS(-10)          -- 10

-- 指数对数
POW(x, y)
POWER(x, y)
SQRT(x)
EXP(x)
LN(x)
LOG(x)
LOG2(x)
LOG10(x)

-- 三角函数
SIN(x)
COS(x)
TAN(x)
ASIN(x)
ACOS(x)
ATAN(x)
ATAN2(y, x)

-- 舍入
ROUND(x, d)
FLOOR(x)
CEIL(x)
CEILING(x)
TRUNCATE(x, d)

-- 随机
RAND()
RAND(n)

-- 符号
SIGN(x)

-- 取模
MOD(x, y)
```

### 条件函数

```sql
-- CASE
CASE WHEN condition THEN result1 ELSE result2 END
CASE expr WHEN value1 THEN result1 ELSE result2 END

-- IF
IF(cond, true_val, false_val)

-- IFNULL
IFNULL(col, default_val)

-- NULLIF
NULLIF(col1, col2)

-- COALESCE
COALESCE(col1, col2, default_val)
```

### 类型转换

```sql
-- CAST
CAST(col AS TYPE)
CONVERT(col, TYPE)

-- 常用转换
CAST(col AS INT)
CAST(col AS VARCHAR)
CAST(col AS DATE)
CAST(col AS DATETIME)
CAST(col AS DECIMAL(10, 2))

-- 字符串转数字
CAST('123' AS INT)
CAST('123.45' AS DECIMAL(10, 2))
```

### 分析函数

```sql
-- 窗口函数
SUM() OVER (PARTITION BY col ORDER BY col)
AVG() OVER (PARTITION BY col ORDER BY col)
COUNT() OVER (PARTITION BY col ORDER BY col)

-- 排名函数
RANK() OVER (ORDER BY col)
DENSE_RANK() OVER (ORDER BY col)
ROW_NUMBER() OVER (ORDER BY col)
PERCENT_RANK() OVER (ORDER BY col)

-- 前后值
LAG(col) OVER (ORDER BY col)
LEAD(col) OVER (ORDER BY col)

-- 头部
FIRST_VALUE(col) OVER (PARTITION BY col ORDER BY col)
LAST_VALUE(col) OVER (PARTITION BY col ORDER BY col)

-- 分布
CUME_DIST() OVER (ORDER BY col)
NTILE(n) OVER (ORDER BY col)

-- 上下界
PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY col)
PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY col)
```

---

## 系统变量

### 会话变量

```sql
-- 查看所有变量
SHOW VARIABLES;

-- 查看特定变量
SHOW VARIABLES LIKE '%batch%';

-- 设置会话变量
SET batch_size = 10000;
SET exec_mem_limit = 8G;
SET parallel_fragment_exec_instance_num = 4;

-- 查看值
SELECT @@batch_size;
```

### 全局变量

```sql
-- 设置全局变量 (需要管理员权限)
SET GLOBAL exec_mem_limit = 16G;

-- 持久化全局变量
SET PERSIST exec_mem_limit = 16G;
```

### 常用变量

```sql
-- 执行参数
SET parallel_fragment_exec_instance_num = 4;  -- 并行度
SET enable_spilling = true;                    -- 允许溢出
SET batch_size = 4096;                        -- 批次大小

-- 查询优化
SET enable_cost_based_join_reorder = true;    -- 基于成本的 Join 重排
SET enable_projection = true;                  -- 列裁剪

-- 内存控制
SET exec_mem_limit = 8G;                      -- 执行内存限制
SET load_mem_limit = 2G;                      -- 导入内存限制

-- 超时控制
SET query_timeout = 300;                      -- 查询超时(秒)
SET insert_timeout = 300;                     -- 插入超时(秒)

-- 其他
SET forward_to_master = false;                -- 转发到 Master
SET time_zone = 'Asia/Shanghai';              -- 时区
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | Doris 架构详解 |
| [02-data-model.md](02-data-model.md) | 数据模型详解 |
| [03-data-operations.md](03-data-operations.md) | 数据操作指南 |
| [05-operations.md](05-operations.md) | 运维指南 |
| [README.md](README.md) | 索引文档 |
