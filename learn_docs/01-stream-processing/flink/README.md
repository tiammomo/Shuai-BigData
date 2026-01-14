# Flink 中文文档

> Apache Flink 是一个分布式流处理框架，用于有状态计算和无界/有界数据处理。

## 文档列表

| 文档 | 说明 | 关键内容 |
|------|------|---------|
| [01-architecture.md](01-architecture.md) | 架构详解 | JobManager、TaskManager、Slot、Checkpoint、内存模型 |
| [02-datastream.md](02-datastream.md) | DataStream API | 数据源、转换算子、窗口、异步 IO |
| [03-table-sql.md](03-table-sql.md) | Table API / SQL | 表定义、查询、窗口聚合、连接操作 |
| [04-state-checkpoint.md](04-state-checkpoint.md) | 状态管理 | Keyed State、Operator State、RocksDB、Checkpoint |
| [05-cep.md](05-cep.md) | 复杂事件处理 | 模式定义、模式检测、事件匹配 |
| [06-operations.md](06-operations.md) | 运维指南 | 集群部署、作业管理、监控告警、故障恢复 |

## 快速入门

### 核心概念

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Flink 集群架构                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      JobManager                                  │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │   │
│  │  │ Dispatcher      │  │ Resource        │  │ JobMaster       │  │   │
│  │  │ - REST API     │  │ Manager         │  │ - 作业调度      │  │   │
│  │  │ - 提交 Job     │  │ - 资源管理      │  │ - Checkpoint   │  │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                     ┌──────────────┼──────────────┐                    │
│                     ▼              ▼              ▼                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      TaskManager (多个)                          │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │   │
│  │  │ Task   │  │ Task   │  │ Task   │  │ Task   │            │   │
│  │  │ Slot 1 │  │ Slot 2 │  │ Slot 3 │  │ Slot 4 │            │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │   │
│  │                                                                  │   │
│  │  - 内存管理     - 网络栈     - Task 执行   - 状态存储            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 代码示例

```java
// DataStream API
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);

DataStream<String> stream = env
    .socketTextStream("localhost", 9999)
    .map(String::toLowerCase)
    .filter(s -> s.length() > 0)
    .keyBy(s -> s)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .sum(1);

stream.print();

env.execute("Flink Job");

// Table API / SQL
TableEnvironment tableEnv = TableEnvironment.create(settings);

tableEnv.executeSql(
    "CREATE TABLE orders (" +
    "  order_id BIGINT," +
    "  amount DECIMAL(10, 2)," +
    "  order_time TIMESTAMP(3)" +
    ") WITH ('connector' = 'kafka', ...)");

tableEnv.executeSql(
    "SELECT customer_id, SUM(amount) AS total " +
    "FROM orders " +
    "GROUP BY customer_id");
```

## 文档导航

### 入门

1. 先阅读 [01-architecture.md](01-architecture.md) 了解核心架构
2. 学习 DataStream API [02-datastream.md](02-datastream.md)
3. 了解 Table API 和 SQL [03-table-sql.md](03-table-sql.md)

### 进阶

4. 掌握状态管理 [04-state-checkpoint.md](04-state-checkpoint.md)
5. 学习复杂事件处理 [05-cep.md](05-cep.md)
6. 生产环境运维 [06-operations.md](06-operations.md)

## 版本兼容性

| Flink 版本 | Scala 版本 | 建议 Java |
|-----------|-----------|-----------|
| 1.18.x | 2.12 | 11/17 |
| 1.17.x | 2.12 | 11 |
| 1.16.x | 2.12 | 11 |

## 相关资源

- [官方文档](https://nightlies.apache.org/flink/flink-docs-stable/)
