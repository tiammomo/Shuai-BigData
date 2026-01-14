# Apache Fluss 中文文档

> Fluss 是一个面向实时数据流的分布式计算引擎，专注于低延迟流处理。

## 目录

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |

## 快速入门

### Maven 依赖

```xml
<dependency>
    <groupId>org.apache.fluss</groupId>
    <artifactId>fluss-core</artifactId>
    <version>1.0.0</version>
</dependency>
<dependency>
    <groupId>org.apache.fluss</groupId>
    <artifactId>fluss-runtime</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 核心特性

```
┌─────────────────────────────────────────────────────────────────┐
│                     Fluss 核心特性                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    低延迟流处理                           │   │
│  │  - 毫秒级延迟                                            │   │
│  │  - 高吞吐                                                │   │
│  │  - 精确一次语义                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    统一 API                               │   │
│  │  - DataStream API                                       │   │
│  │  - Table API                                            │   │
│  │  - SQL                                                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    状态管理                               │   │
│  │  - 分布式状态                                            │   │
│  │  - 增量 Checkpoint                                      │   │
│  │  - 状态恢复                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    时间处理                               │   │
│  │  - 事件时间                                              │   │
│  │  - 水印策略                                              │   │
│  │  - 迟到数据处理                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 数据流架构

```
┌─────────────────────────────────────────────────────────────────┐
│                     Fluss 数据流                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐ │
│  │  Source  │ -> │ Transform│ -> │  Window  │ -> │   Sink   │ │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘ │
│       │              │               │               │          │
│       v              v               v               v          │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐ │
│  │  Kafka   │    │  Map/    │    │ Tumbling │    │  Kafka   │ │
│  │  Pulsar  │    │ FlatMap  │    │ Sliding  │    │  ES      │ │
│  │  Custom  │    │ Filter   │    │ Session  │    │  JDBC    │ │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Java 示例

```java
import org.apache.fluss.streaming.api.DataStreamAPI;
import org.apache.fluss.streaming.api.environment.StreamExecutionEnvironment;

public class FlussExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从 Kafka 读取
        DataStream<String> source = env
            .fromSource(
                new KafkaSource<>("localhost:9092", "topic", "group-id"),
                WatermarkStrategy.noWatermarks()
            )
            .name("Kafka Source");

        // 转换
        DataStream<Order> orders = source
            .map(JSON::parseObject)
            .map(json -> new Order(json.getLong("id"), json.getDouble("amount")))
            .name("Parse Orders");

        // 窗口聚合
        DataStream<Revenue> revenue = orders
            .keyBy(Order::getId)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
            .aggregate(new RevenueAggregator())
            .name("Revenue Window");

        // 输出
        revenue.sinkTo(new JdbcSink<>("jdbc:mysql://localhost:3306/db"));

        env.execute("Fluss Example");
    }
}
```

## 相关资源

- [官方文档](https://fluss.apache.org/) (待发布)
- [GitHub](https://github.com/apache/fluss)
