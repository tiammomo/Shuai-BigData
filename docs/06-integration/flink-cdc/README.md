# Flink CDC 中文文档

> Flink CDC 是 Flink 生态的变更数据捕获工具，支持数据库实时同步。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |

## 快速入门

```java
// Maven 依赖
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-mysql-cdc</artifactId>
    <version>3.0.1</version>
</dependency>
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     Flink CDC 核心特性                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      全量读取                            │   │
│  │  - 首次同步全量数据                                      │   │
│  │  - Snapshot 阶段                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      增量读取                            │   │
│  │  - Binlog 监听                                           │   │
│  │  - 实时增量同步                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Exactly-Once                        │   │
│  │  - 端到端一致性                                          │   │
│  │  - Checkpoint 保证                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Schema Evolution                    │   │
│  │  - Schema 变更支持                                       │   │
│  │  - 自动同步                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## DataStream API 示例

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.mysql.source.MySqlSource;
import org.apache.flink.connector.mysql.source.assigners.MySqlBinlogSplitAssigner;
import org.apache.flink.connector.mysql.source.config.MySqlSourceOptions;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
    .hostname("localhost")
    .port(3306)
    .databaseList("mydb")
    .tableList("mydb.users")
    .username("root")
    .password("password")
    .deserializer(new JsonDebeziumDeserializationSchema())
    .build();

env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
   .print();

env.execute("MySQL CDC");
```

## 支持的数据库

| 数据库 | 连接器 | 支持版本 |
|-------|-------|---------|
| MySQL | flink-connector-mysql-cdc | 5.7, 8.0 |
| PostgreSQL | flink-connector-postgres-cdc | 9.4+ |
| MongoDB | flink-connector-mongodb-cdc | 3.6+ |
| Oracle | flink-connector-oracle-cdc | 11g+ |
| SQL Server | flink-connector-sqlserver-cdc | 2016+ |

## 相关资源

- [官方文档](https://ververica.github.io/flink-cdc/)
- [01-architecture.md](01-architecture.md)
