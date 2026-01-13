# InfluxDB 数据操作

## 目录

- [数据写入](#数据写入)
- [InfluxQL 查询](#influxql-查询)
- [数据保留策略](#数据保留策略)
- [连续查询](#连续查询)
- [Downsampling](#downsampling)

---

## 数据写入

### Line Protocol

```bash
# 格式: measurement,tag_key=tag_value field_key=field_value timestamp

# 基础格式
temperature,host=server01,region=us-west value=25.5 1704880800000000000

# 多个字段
cpu,host=server01,core=0 usage_user=25.5,usage_system=10.2,idle=64.3 1704880800000000000

# 只有时间戳 (补全字段)
temperature value=22.5

# 整数类型 (加 i)
request_count,endpoint=/api/users count=1000i 1704880800000000000

# 布尔类型
active,server=server01 online=true

# 字符串字段
event,type=login user="alice" "message"="User logged in"

# 转义空格和逗号
measurement\ with\ space,tag=value field="value with, comma" 1704880800000000000
```

### HTTP API 写入

```bash
# 单条写入
curl -X POST "http://localhost:8086/api/v2/write?org=my-org&bucket=my-bucket&precision=ns" \
  --header "Authorization: Token my-token" \
  --data-raw 'temperature,host=server01 value=25.5'

# 批量写入
curl -X POST "http://localhost:8086/api/v2/write?org=my-org&bucket=my-bucket" \
  --header "Authorization: Token my-token" \
  --data-raw '
temperature,host=server01 value=25.5 1704880800000000000
temperature,host=server02 value=26.0 1704880800000000001
cpu,host=server01 usage=75.5 1704880800000000000
memory,host=server01 used=8192 1704880800000000000
'
```

### Java 客户端写入

```java
// InfluxDB 2.x Java Client
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

InfluxDBClient client = InfluxDBClientFactory.create(
    "http://localhost:8086", "my-token".toCharArray());

WriteApi writeApi = client.getWriteApi();

// 使用 Point
Point point = Point.measurement("temperature")
    .addTag("host", "server01")
    .addTag("region", "us-west")
    .addField("value", 25.5)
    .time(System.currentTimeMillis(), WritePrecision.MS);

writeApi.writePoint("my-bucket", "my-org", point);

// 批量写入点
writeApi.writePoints(
    "my-bucket", "my-org",
    Arrays.asList(
        Point.measurement("cpu").addField("usage", 75.5).time(System.currentTimeMillis(), WritePrecision.MS),
        Point.measurement("memory").addField("used", 8192L).time(System.currentTimeMillis(), WritePrecision.MS)
    )
);

// 使用 POJO
writeApi.writeMeasurement("my-bucket", "my-org", WritePrecision.MS, temperature);
```

---

## InfluxQL 查询

### 基本查询

```sql
-- 查询测量
SELECT * FROM temperature;

-- 带时间范围
SELECT * FROM temperature
WHERE time >= '2024-01-10T00:00:00Z'
  AND time < '2024-01-11T00:00:00Z';

-- 最近数据
SELECT * FROM temperature
ORDER BY time DESC
LIMIT 10;
```

### 聚合函数

```sql
-- 时间聚合
SELECT mean(value) FROM temperature
WHERE time >= now() - 1h
GROUP BY time(1m);

-- 窗口聚合
SELECT
  mean(value) as avg_temp,
  max(value) as max_temp,
  min(value) as min_temp
FROM temperature
WHERE time >= '2024-01-10'
GROUP BY host, time(5m);

-- 百分比
SELECT percentile(value, 95)
FROM temperature
WHERE time >= now() - 1d;

-- 计数
SELECT count(value) FROM temperature
WHERE time >= now() - 1h
GROUP BY time(1m);
```

### 连续查询

```sql
-- 创建连续查询
CREATE CONTINUOUS QUERY cq_1m_avg
ON my_database
BEGIN
  SELECT mean(value) AS avg_value
  INTO "temperature_1m_avg"
  FROM "temperature"
  GROUP BY time(1m), host
END;

-- 查看连续查询
SHOW CONTINUOUS QUERIES;

-- 删除连续查询
DROP CONTINUOUS QUERY cq_1m_avg ON my_database;
```

---

## 数据保留策略

### 创建保留策略

```sql
-- 创建保留策略
CREATE RETENTION POLICY "30_days"
ON my_database
DURATION 30d
REPLICATION 1;

-- 创建无限期保留策略
CREATE RETENTION POLICY "forever"
ON my_database
DURATION INF
REPLICATION 1;

-- 修改保留策略
ALTER RETENTION POLICY "30_days"
ON my_database
DURATION 60d
SHARD DURATION 7d;

-- 删除保留策略
DROP RETENTION POLICY "30_days" ON my_database;
```

### 绑定默认策略

```sql
-- 设置默认保留策略
ALTER DATABASE my_database
SET retention POLICY "30_days";

-- 查看数据库的保留策略
SHOW RETENTION POLICIES ON my_database;
```

---

## Downsampling

### 自动下采样

```sql
-- 创建下采样测量
CREATE MEASUREMENT "temperature_1h_avg"
AS SELECT mean(value) AS avg_value,
          max(value) AS max_value,
          min(value) AS min_value
FROM "temperature"
GROUP BY time(1h), host;

-- 连续查询实现下采样
CREATE CONTINUOUS QUERY cq_downsample_1h
ON my_database
BEGIN
  SELECT mean(value) AS avg_value,
         max(value) AS max_value,
         min(value) AS min_value
  INTO "temperature_1h_avg"
  FROM "temperature"
  GROUP BY time(1h), host
END;
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-query-guide.md](03-query-guide.md) | 查询指南 |
| [README.md](README.md) | 索引文档 |
