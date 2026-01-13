# Doris 数据操作指南

## 目录

- [数据导入](#数据导入)
- [数据导出](#数据导出)
- [数据查询](#数据查询)
- [数据修改](#数据修改)
- [事务操作](#事务操作)

---

## 数据导入

### INSERT 导入

```sql
-- 1. INSERT VALUES
INSERT INTO users (user_id, name, age, gender)
VALUES (1001, 'Alice', 25, 'F'),
       (1002, 'Bob', 30, 'M');

-- 2. INSERT SELECT
INSERT INTO users (user_id, name, age, gender)
SELECT user_id, name, age, gender
FROM staging_users
WHERE status = 'active';

-- 3. INSERT ON DUPLICATE KEY UPDATE
INSERT INTO users (user_id, name, age)
VALUES (1001, 'Alice', 25)
ON DUPLICATE KEY UPDATE
    name = 'Alice',
    age = 25;

-- 4. INSERT IGNORE
INSERT IGNORE INTO users (user_id, name, age)
VALUES (1001, 'Alice', 25);

-- 5. SET 语法
INSERT INTO users
SET user_id = 1001,
    name = 'Alice',
    age = 25;

-- 6. 批量插入
INSERT INTO orders (order_id, user_id, amount, order_time)
VALUES
    (1, 1001, 99.99, NOW()),
    (2, 1002, 149.99, NOW()),
    (3, 1001, 199.99, NOW());
```

### Stream Load

```bash
# 1. HTTP POST 导入
curl --location-trusted -u admin:password \
    -H "label: load_20240115_001" \
    -H "column_separator:," \
    -H "columns: user_id, name, age, gender" \
    -T /path/to/data.csv \
    http://fe_host:8030/api/test_db/users/_stream_load

# 2. JSON 格式导入
curl --location-trusted -u admin:password \
    -H "label: load_20240115_002" \
    -H "format: json" \
    -H "jsonpaths: [\"$.user_id\", \"$.name\", \"$.age\"]" \
    -H "strip_outer_array: true" \
    -T /path/to/data.json \
    http://fe_host:8030/api/test_db/users/_stream_load

# JSON 数据示例:
# [{"user_id": 1001, "name": "Alice", "age": 25},
#  {"user_id": 1002, "name": "Bob", "age": 30}]

# 3. 导入参数
--location-trusted    # 允许重定向
-H "max_filter_ratio: 0.1"    # 最大过滤比例
-H "timeout: 300"             # 超时时间(秒)
-H "strict_mode: true"        # 严格模式
```

### Broker Load

```sql
-- 1. 从 HDFS 导入
LOAD LABEL test_db.load_20240115_001 (
    DATA INFILE("hdfs://namenode:9000/data/*.csv")
    INTO TABLE users
    COLUMNS TERMINATED BY ","
    (user_id, name, age, gender)
)
WITH BROKER hdfs_broker (
    "hadoop.username" = "hdfs",
    "dfs.nameservices" = "mycluster",
    "dfs.ha.namenodes.mycluster" = "nn1,nn2",
    "dfs.namenode.rpc-address.mycluster.nn1" = "namenode1:8030",
    "dfs.namenode.rpc-address.mycluster.nn2" = "namenode2:8030"
);

-- 2. 从 S3 导入
LOAD LABEL test_db.load_20240115_002 (
    DATA INFILE("s3://bucket/data/*.csv")
    INTO TABLE users
    COLUMNS TERMINATED BY ","
    (user_id, name, age, gender)
)
WITH BROKER s3_broker (
    "aws.access_key" = "xxx",
    "aws.secret_key" = "yyy",
    "aws.endpoint" = "s3.amazonaws.com"
);

-- 3. 导入参数
LOAD LABEL test_db.load_20240115_003 (
    DATA INFILE("hdfs://...")
    INTO TABLE users
    COLUMNS TERMINATED BY ","
    (user_id, name, age, gender)
)
PROPERTIES (
    "timeout" = "3600",
    "max_filter_ratio" = "0.1",
    "strict_mode" = "true"
);

-- 4. 查看导入状态
SHOW LOAD FROM test_db;
SHOW LOAD WHERE LABEL = "load_20240115_001";

-- 5. 取消导入
CANCEL LOAD FROM test_db WHERE LABEL = "load_20240115_001";
```

### Routine Load

```sql
-- 1. 创建 Kafka 例行导入
CREATE ROUTINE LOAD test_db.kafka_load_001 ON users
PROPERTIES (
    "desired_concurrent_number" = "5",
    "max_batch_interval" = "20",
    "max_batch_rows" = "300000",
    "max_error_number" = "100",
    "strict_mode" = "true",
    "format" = "json"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka1:9092,kafka2:9092",
    "kafka_topic" = "users_topic",
    "kafka_partitions" = "0,1,2,3",
    "kafka_offsets" = "OFFSET_BEGINNING,OFFSET_BEGINNING,OFFSET_BEGINNING,OFFSET_BEGINNING"
);

-- 2. 查看例行导入
SHOW ROUTINE LOAD;
SHOW ROUTINE LOAD FOR test_db.kafka_load_001;

-- 3. 暂停/恢复例行导入
PAUSE ROUTINE LOAD test_db.kafka_load_001;
RESUME ROUTINE LOAD test_db.kafka_load_001;

-- 4. 修改例行导入
ALTER ROUTINE LOAD test_db.kafka_load_001
PROPERTIES ("desired_concurrent_number" = "10");

-- 5. 停止例行导入
STOP ROUTINE LOAD test_db.kafka_load_001;

-- 6. Kafka 数据格式
-- JSON 格式
{"user_id": 1001, "name": "Alice", "age": 25}
{"user_id": 1002, "name": "Bob", "age": 30}

-- CSV 格式
1001,Alice,25
1002,Bob,30
```

### Sync Load

```sql
-- 1. 创建同步导入任务
CREATE SYNC LOAD FROM KAFKA (
    "kafka_broker_list" = "kafka1:9092",
    "kafka_topic" = "orders_topic",
    "kafka_partitions" = "0",
    "kafka_offsets" = "OFFSET_BEGINNING"
)
INTO TABLE orders
COLUMNS TERMINATED BY ","
SET (
    order_id = COLUMN(0),
    user_id = COLUMN(1),
    amount = COLUMN(2),
    order_time = FROM_UNIXTIME(COLUMN(3))
);

-- 2. 查看同步导入
SHOW SYNC LOAD;
```

---

## 数据导出

### Export 导出

```sql
-- 1. 创建导出任务
EXPORT TABLE test_db.users
TO "hdfs://namenode:9000/export/users"
PROPERTIES (
    "column_separator" = ",",
    "columns" = "user_id,name,age,gender",
    "timeout" = "3600"
)
WITH BROKER hdfs_broker (
    "hadoop.username" = "hdfs"
);

-- 2. 查看导出任务
SHOW EXPORT FROM test_db;

-- 3. 取消导出任务
CANCEL EXPORT FROM test_db WHERE ID = 1;

-- 4. 导出查询结果
EXPORT TABLE (
    SELECT user_id, name, age
    FROM users
    WHERE age >= 18
) TO "hdfs://namenode:9000/export/adult_users"
PROPERTIES ("format" = "csv");
```

### SELECT INTO OUTFILE

```sql
-- 1. 导出到 HDFS
SELECT * FROM users
INTO OUTFILE "hdfs://namenode:9000/export/users_20240115"
FORMAT AS CSV
PROPERTIES (
    "column_separator" = ",",
    "line_delimiter" = "\n",
    "broker" = "hdfs_broker",
    "broker.username" = "hdfs",
    "fs.hdfs.impl" = "org.apache.hadoop.hdfs.DistributedFileSystem"
);

-- 2. 导出到 S3
SELECT * FROM users
INTO OUTFILE "s3://bucket/export/users.csv"
FORMAT AS CSV
PROPERTIES (
    "column_separator" = ",",
    "aws.access_key" = "xxx",
    "aws.secret_key" = "yyy"
);

-- 3. 带条件的导出
SELECT user_id, name, age, gender
FROM users
WHERE create_time >= '2024-01-01'
INTO OUTFILE "hdfs://..."
FORMAT AS CSV
PROPERTIES ("column_separator" = "\t");

-- 4. 导出参数
SELECT * FROM users
INTO OUTFILE "hdfs://..."
PROPERTIES (
    "max_file_size" = "512MB",        # 单文件最大
    "parallelism" = "5",               # 并行度
    "header" = "true",                # 导出列名
    "null_format" = "NULL"            # NULL 值格式
);
```

---

## 数据查询

### 基础查询

```sql
-- 1. 简单查询
SELECT * FROM users;
SELECT user_id, name, age FROM users;

-- 2. 条件过滤
SELECT * FROM users WHERE age >= 18;
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE age IN (18, 25, 30);

-- 3. 聚合查询
SELECT gender, COUNT(*) as count, AVG(age) as avg_age
FROM users
GROUP BY gender;

SELECT
    DATE(order_time) as order_date,
    COUNT(*) as orders,
    SUM(amount) as revenue
FROM orders
GROUP BY DATE(order_time)
ORDER BY order_date DESC
LIMIT 30;

-- 4. JOIN 查询
SELECT
    o.order_id,
    o.amount,
    u.name,
    u.email
FROM orders o
JOIN users u ON o.user_id = u.user_id
WHERE o.order_time >= '2024-01-01';

-- 5. 子查询
SELECT * FROM users
WHERE user_id IN (SELECT user_id FROM orders WHERE amount > 100);

SELECT u.*,
    (SELECT SUM(amount) FROM orders WHERE user_id = u.user_id) as total_amount
FROM users u;
```

### 高级查询

```sql
-- 1. 窗口函数
SELECT
    user_id,
    order_time,
    amount,
    SUM(amount) OVER (PARTITION BY user_id ORDER BY order_time) as running_total,
    AVG(amount) OVER (PARTITION BY user_id ORDER BY order_time ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg
FROM orders;

-- 2. LEAD/LAG
SELECT
    user_id,
    order_time,
    amount,
    LAG(amount) OVER (PARTITION BY user_id ORDER BY order_time) as prev_amount,
    LEAD(amount) OVER (PARTITION BY user_id ORDER BY order_time) as next_amount
FROM orders;

-- 3. RANK
SELECT
    user_id,
    amount,
    RANK() OVER (ORDER BY amount DESC) as rank,
    DENSE_RANK() OVER (ORDER BY amount DESC) as dense_rank,
    ROW_NUMBER() OVER (ORDER BY amount DESC) as row_num
FROM orders;

-- 4. 分位数
SELECT
    amount,
    PERCENT_RANK() OVER (ORDER BY amount) as pct_rank
FROM orders;

SELECT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY amount) as median_disc
FROM orders;

-- 5. GROUPING SETS
SELECT
    user_id,
    DATE(order_time) as order_date,
    status,
    SUM(amount) as total
FROM orders
GROUP BY GROUPING SETS (
    (user_id),
    (DATE(order_time)),
    (user_id, DATE(order_time)),
    (user_id, status)
);

-- 6. WITH ROLLUP
SELECT
    user_id,
    DATE(order_time) as order_date,
    SUM(amount) as total
FROM orders
GROUP BY user_id, DATE(order_time) WITH ROLLUP;

-- 7. WITH CUBE
SELECT
    user_id,
    status,
    SUM(amount) as total
FROM orders
GROUP BY user_id, status WITH CUBE;
```

### JOIN 优化

```sql
-- 1. 查看 Join 顺序
EXPLAIN SELECT
    o.order_id,
    u.name,
    p.product_name
FROM orders o
JOIN users u ON o.user_id = u.user_id
JOIN products p ON o.product_id = p.product_id;

-- 2. Colocate Join
-- 创建 colocate 表
CREATE TABLE users (
    user_id BIGINT NOT NULL,
    name VARCHAR(50)
) UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "3", "colocate_with" = "group1");

CREATE TABLE orders (
    order_id BIGINT NOT NULL,
    user_id BIGINT,
    amount DECIMAL(10, 2)
) UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "3", "colocate_with" = "group1");

-- 3. Bucket Shuffle Join
-- 自动优化，Join 字段与分桶字段一致时使用
SET enable_bucket_shuffle_join = true;

-- 4. BROADCAST Join
-- 小表广播
SELECT /*+ BROADCAST(u) */
    o.order_id,
    u.name
FROM orders o
JOIN users u ON o.user_id = u.user_id;
```

---

## 数据修改

### UPDATE 操作

```sql
-- 1. 简单 UPDATE
UPDATE users
SET age = age + 1
WHERE user_id = 1001;

-- 2. 条件 UPDATE
UPDATE users
SET status = 'active',
    last_login = NOW()
WHERE status = 'inactive'
  AND last_login < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- 3. 批量 UPDATE
UPDATE users
SET age = CASE user_id
    WHEN 1001 THEN 26
    WHEN 1002 THEN 31
    WHEN 1003 THEN 28
END
WHERE user_id IN (1001, 1002, 1003);

-- 4. JOIN UPDATE
UPDATE orders o
INNER JOIN users u ON o.user_id = u.user_id
SET o.user_name = u.name
WHERE o.status = 'pending';

-- 5. 基于 SELECT 的 UPDATE
UPDATE orders
SET status = 'cancelled',
    cancel_time = NOW()
WHERE user_id IN (
    SELECT user_id FROM users WHERE status = 'banned'
);
```

### DELETE 操作

```sql
-- 1. 简单 DELETE
DELETE FROM users WHERE user_id = 1001;

-- 2. 批量 DELETE
DELETE FROM orders
WHERE order_time < DATE_SUB(NOW(), INTERVAL 1 YEAR);

-- 3. 条件 DELETE
DELETE FROM orders
WHERE status = 'completed'
  AND order_time < DATE_SUB(NOW(), INTERVAL 90 DAY);

-- 4. JOIN DELETE
DELETE o FROM orders o
INNER JOIN users u ON o.user_id = u.user_id
WHERE u.status = 'banned';

-- 5. 基于 SELECT 的 DELETE
DELETE FROM orders
WHERE user_id IN (
    SELECT user_id FROM users WHERE create_time < '2023-01-01'
    AND (SELECT COUNT(*) FROM orders WHERE user_id = users.user_id) < 5
);

-- 6. 删除分区数据 (高效)
ALTER TABLE orders DROP PARTITION p202301;
```

### 批量修改

```sql
-- 1. INSERT OVERWRITE
INSERT OVERWRITE TABLE users
PARTITION (year='2024')
SELECT
    user_id,
    name,
    age,
    gender,
    NOW() as create_time
FROM staging_users
WHERE status = 'active';

-- 2. INSERT OVERWRITE PARTITION
INSERT OVERWRITE TABLE users PARTITION(year='2024', month='01')
SELECT * FROM staging_users;

-- 3. REPLACE
REPLACE INTO users (user_id, name, age)
VALUES (1001, 'Alice', 26);
```

---

## 事务操作

### 原子操作

```sql
-- 1. INSERT ON DUPLICATE KEY (原子 Upsert)
INSERT INTO users (user_id, name, age, status)
VALUES (1001, 'Alice', 25, 'active')
ON DUPLICATE KEY UPDATE
    name = 'Alice',
    age = 25,
    status = 'active',
    update_time = NOW();

-- 2. REPLACE INTO (原子替换)
-- 如果存在则删除旧记录，插入新记录
REPLACE INTO users (user_id, name, age)
VALUES (1001, 'Alice', 26);

-- 3. INSERT IGNORE (原子插入)
-- 如果存在则忽略
INSERT IGNORE INTO users (user_id, name, age)
VALUES (1001, 'Alice', 25);

-- 4. 条件删除+插入 (模拟事务)
DELETE FROM users WHERE user_id = 1001;
INSERT INTO users (user_id, name, age) VALUES (1001, 'Alice', 26);
```

### 并发控制

```sql
-- 1. 乐观锁 (使用版本号)
UPDATE users
SET age = 26,
    version = version + 1
WHERE user_id = 1001 AND version = 5;

-- 2. 悲观锁 (SELECT FOR UPDATE)
-- Doris 不支持 FOR UPDATE

-- 3. 使用时间戳
UPDATE users
SET age = 26,
    update_time = NOW()
WHERE user_id = 1001
  AND update_time < DATE_SUB(NOW(), INTERVAL 1 HOUR);
```

### 数据一致性

```sql
-- 1. 查看数据副本
ADMIN SHOW REPLICA STATUS FROM users;

-- 2. 检查数据完整性
CHECK TABLE users;

-- 3. 修复副本
ADMIN REPAIR TABLE users;

-- 4. 查看 Tablet 健康度
SHOW PROC '/cluster_health/tablet_health';

-- 5. 手动触发副本均衡
ADMIN BALANCE TABLE users;
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | Doris 架构详解 |
| [02-data-model.md](02-data-model.md) | 数据模型详解 |
| [04-sql-reference.md](04-sql-reference.md) | SQL 参考 |
| [05-operations.md](05-operations.md) | 运维指南 |
| [README.md](README.md) | 索引文档 |
