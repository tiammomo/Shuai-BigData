# Flink CDC 架构详解

## 目录

- [概述](#概述)
- [核心概念](#核心概念)
- [架构原理](#架构原理)
- [连接器](#连接器)
- [数据同步](#数据同步)

---

## 概述

Flink CDC (Change Data Capture) 是基于 Flink 的实时数据捕获框架。

### 核心特性

| 特性 | 说明 |
|------|------|
| **实时捕获** | 毫秒级延迟 |
| **全增量一体化** | 全量 + 增量同步 |
| **无锁读取** | 不影响源库 |
| **断点续传** | 支持 Checkpoint |
| **多源支持** | MySQL, Postgres, Oracle |

---

## 核心概念

### 核心术语

```
┌─────────────────────────────────────────────────────────────────┐
│                    Flink CDC 核心概念                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  CDC           │  Change Data Capture                          │
│  Binlog        │  MySQL 二进制日志                             │
│  WAL           │  Write Ahead Log                              │
│  Snapshot      │  全量快照                                     │
│  Change Events │  变更事件 (Insert/Update/Delete)              │
│  Debezium      │  CDC 底层引擎                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 事件类型

| 事件 | 说明 |
|------|------|
| Insert | 新增数据 |
| Update_Before | 更新前数据 |
| Update_After | 更新后数据 |
| Delete | 删除数据 |

---

## 架构原理

### 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                    Flink CDC 架构                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Source Database                       │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐                  │   │
│  │  │ MySQL   │  │Postgres │  │ Oracle  │                  │   │
│  │  │ Binlog  │  │ WAL     │  │ LogMiner│                  │   │
│  │  └─────────┘  └─────────┘  └─────────┘                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Debezium Engine                       │   │
│  │  - 日志解析                                              │   │
│  │  - 事件转换                                              │   │
│  │  - 顺序保证                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Flink Source                          │   │
│  │  ┌───────────────────────────────────────────────────┐  │   │
│  │  │  MySQLSource → DebeziumSource → PostgreSQLSource │  │   │
│  │  └───────────────────────────────────────────────────┘  │   │
│  │  - 并行读取                                              │   │
│  │  - Checkpoint                                           │   │
│  │  - 故障恢复                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Flink Pipeline                        │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐                  │   │
│  │  │  Transform │  │  Filter │  │  Agregate│              │   │
│  │  └─────────┘  └─────────┘  └─────────┘                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Sink (Kafka, DB, etc)                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 工作流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    Flink CDC 工作流程                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. 全量阶段                                                    │
│     - 读取历史数据                                              │
│     - 并行加载                                                  │
│     - 记录位置                                                  │
│                                                                 │
│  2. 增量阶段                                                    │
│     - 切换到 Binlog 读取                                        │
│     - 实时捕获变更                                              │
│     - 与全量数据合并                                            │
│                                                                 │
│  3. 同步阶段                                                    │
│     - 写入目标端                                                │
│     - 保证一致性                                                │
│     - 故障恢复                                                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 连接器

### MySQL CDC

```java
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class MySqlCdcExample {

    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("localhost")
            .port(3306)
            .databaseList("mydb")
            .tableList("mydb.orders")
            .username("root")
            .password("password")
            .deserializer(new JsonDebeziumDeserializationSchema())
            .startupOptions(StartupOptions.latest())
            .serverTimeZone("Asia/Shanghai")
            .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC")
           .print();
    }
}
```

### PostgreSQL CDC

```java
import com.ververica.cdc.connectors.postgres.source.PostgresSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class PostgresCdcExample {

    public static void main(String[] args) throws Exception {
        PostgresSource<String> postgresSource = PostgresSource.<String>builder()
            .hostname("localhost")
            .port(5432)
            .databaseList("mydb")
            .tableList("mydb.users")
            .username("postgres")
            .password("password")
            .deserializer(new JsonDebeziumDeserializationSchema())
            .slotName("flink_cdc")
            .pluginName("pgoutput")
            .build();
    }
}
```

---

## 数据同步

### 整库同步

```java
// 整库同步到 Kafka
MySqlSource<String> source = MySqlSource.<String>builder()
    .hostname("localhost")
    .port(3306)
    .databaseList("mydb")  // 不指定 tableList，同步全库
    .tableList("mydb.*")
    .username("root")
    .password("password")
    .deserializer(new JsonDebeziumDeserializationSchema())
    .build();

// 添加数据库和表信息到消息
// {"database": "mydb", "table": "orders", "type": "insert", "data": {...}}
```

### 增量同步

```java
// 只同步增量数据
MySqlSource<String> source = MySqlSource.<String>builder()
    .hostname("localhost")
    .port(3306)
    .databaseList("mydb")
    .tableList("mydb.orders")
    .username("root")
    .password("password")
    .deserializer(new JsonDebeziumDeserializationSchema())
    .startupOptions(StartupOptions.latest())  // 从最新位置开始
    .build();

// 历史数据同步
MySqlSource<String> source = MySqlSource.<String>builder()
    .hostname("localhost")
    .port(3306)
    .databaseList("mydb")
    .tableList("mydb.orders")
    .username("root")
    .password("password")
    .deserializer(new JsonDebeziumDeserializationSchema())
    .startupOptions(StartupOptions.initial())  // 全量 + 增量
    .build();
```

### 多表同步

```java
// 多表同步到不同 Kafka Topic
DataStream<ChangeEvent> events = env
    .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC")
    .map(JSON::parseObject)
    .map(json -> {
        String table = json.getString("table");
        String data = json.toJSONString();
        return new ChangeEvent(table, data);
    });

// 按表名分流
events.keyBy(event -> event.getTable())
    .map(new SinkToKafka());
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [02-sync-guide.md](02-sync-guide.md) | 同步指南 |
| [03-best-practices.md](03-best-practices.md) | 最佳实践 |
| [README.md](README.md) | 索引文档 |
