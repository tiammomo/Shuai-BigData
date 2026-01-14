# InfluxDB 中文文档

> InfluxDB 是一个开源的时序数据库，专为时间序列数据设计。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |

## 快速入门

```java
// Maven 依赖
<dependency>
    <groupId>com.influxdb</groupId>
    <artifactId>influxdb-client-java</artifactId>
    <version>6.12.0</version>
</dependency>
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     InfluxDB 核心概念                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Measurement (测量)                  │   │
│  │  - 类似于表                                             │   │
│  │  - 存储时序数据                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Tag (标签)                          │   │
│  │  - 索引字段                                             │   │
│  │  - 用于过滤和分组                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Field (字段)                        │   │
│  │  - 数据值                                               │   │
│  │  - 必需字段                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Timestamp (时间戳)                  │   │
│  │  - 数据点时间                                           │   │
│  │  - 纳秒精度                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Java API 示例

```java
// 写入数据
WriteOptions writeOptions = WriteOptions.builder()
    .url("http://localhost:8086")
    .token("my-token")
    .org("my-org")
    .bucket("my-bucket")
    .build();

WriteApi writeApi = client.getWriteApi();

Point point = Point.measurement("cpu")
    .addTag("host", "server01")
    .addField("usage_user", 25.5)
    .time(Instant.now(), WritePrecision.NS);

writeApi.writePoint(point);

// 查询数据
Flux flux = Flux.from("cpu")
    .range(-24L, org.influxdb.querybuilder.Arguments.TimeUnit.HOURS);

Query query = new Query(flux.toString(), "my-org");
AsyncQueryServer asyncQueryServer = client.getQueryApi();
```

## 相关资源

- [官方文档](https://docs.influxdata.com/influxdb/v2/)
- [01-architecture.md](01-architecture.md)
