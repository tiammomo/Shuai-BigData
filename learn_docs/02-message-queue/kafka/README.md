# Kafka 中文文档

> Apache Kafka 是一个分布式流处理平台，用于构建实时数据管道和流应用。

## 文档列表

| 文档 | 说明 | 关键内容 |
|------|------|---------|
| [01-architecture.md](01-architecture.md) | 架构详解 | Broker、Partition、Replica、ISR、消费者组 |
| [02-producer.md](02-producer.md) | 生产者指南 | 同步/异步发送、分区策略、序列化、压缩 |
| [03-consumer.md](03-consumer.md) | 消费者指南 | 消费者组、偏移量管理、Rebalance、拦截器 |
| [04-streams.md](04-streams.md) | Streams 指南 | KStream、KTable、状态管理、窗口操作 |
| [05-operations.md](05-operations.md) | 运维指南 | 集群部署、主题管理、监控告警、容量规划 |
| [06-troubleshooting.md](06-troubleshooting.md) | 故障排查 | 生产/消费/性能问题、故障恢复、诊断工具 |

## 快速入门

### 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kafka 核心概念                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Cluster (集群)                                                 │
│  └── Brokers (多个 Broker)                                      │
│      └── Broker 1                                               │
│          └── Topics (多个 Topic)                                │
│              └── Topic: orders                                  │
│                  └── Partitions (多个 Partition)                 │
│                      └── Replicas (副本)                         │
│                          ├── Leader (读写)                       │
│                          └── Followers (同步)                    │
│                                                                 │
│  Consumer Group (消费者组)                                      │
│  └── Consumers (多个 Consumer)                                  │
│      └── Consumer 1 → Partition 0                               │
│      └── Consumer 2 → Partition 1                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 代码示例

```java
// 生产者
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("acks", "all");
props.put("retries", 3);

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("topic", "key", "value"));

// 消费者
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "my-group");
props.put("enable.auto.commit", false);

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("topic"));
consumer.poll(Duration.ofMillis(100));
```

## 文档导航

### 入门

1. 先阅读 [01-architecture.md](01-architecture.md) 了解核心架构
2. 了解生产者发送机制 [02-producer.md](02-producer.md)
3. 了解消费者消费机制 [03-consumer.md](03-consumer.md)

### 开发

4. 学习 Kafka Streams 流处理 [04-streams.md](04-streams.md)
5. 配置生产环境 [05-operations.md](05-operations.md)
6. 故障排查指南 [06-troubleshooting.md](06-troubleshooting.md)

## 版本兼容性

| Flink 版本 | Kafka Client | 建议 Kafka 集群 |
|-----------|-------------|----------------|
| 3.7.x | 3.7.0+ | 3.0+ |
| 3.6.x | 3.6.0+ | 2.8+ |
| 3.5.x | 3.5.0+ | 2.8+ |

## 相关资源

- [官方文档](https://kafka.apache.org/documentation/)
- [Confluent 文档](https://docs.confluent.io/)
