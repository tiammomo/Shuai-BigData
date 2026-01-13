# Apache Kafka 架构详解

## 目录

- [核心概念](#核心概念)
- [集群架构](#集群架构)
- [数据模型](#数据模型)
- [核心术语](#核心术语)
- [分区与副本](#分区与副本)
- [消费者组](#消费者组)

---

## 核心概念

### 什么是 Kafka?

Apache Kafka 是一个分布式流处理平台，具有以下三大核心能力：

| 能力 | 说明 | 场景 |
|------|------|------|
| **发布/订阅** | 消息队列功能 | 系统解耦、异步通信 |
| **持久化** | 高可靠消息存储 | 日志收集、事件溯源 |
| **流处理** | 实时数据处理 | 实时分析、ETL 管道 |

### 核心特性

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kafka 核心特性                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   高吞吐      │  │   高可靠      │  │   高扩展      │         │
│  │  百万级/秒    │  │  副本+持久化   │  │  水平扩展     │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │  消息持久化   │  │   流处理      │  │   多客户端     │         │
│  │  顺序写入     │  │  Kafka Streams│  │  Java/Python │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 集群架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Kafka 集群架构                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                       生产者 (Producer)                          │   │
│   │  - 发送消息到 Topic                                               │   │
│   │  - 选择分区策略                                                   │   │
│   │  - 批量发送 + 压缩                                                 │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    Kafka Broker 集群                             │   │
│   │  ┌───────────┐  ┌───────────┐  ┌───────────┐                    │   │
│   │  │ Broker 1  │  │ Broker 2  │  │ Broker 3  │                    │   │
│   │  │  :9092    │  │  :9092    │  │  :9092    │                    │   │
│   │  │  Leader   │  │  Follower │  │  Follower │                    │   │
│   │  └───────────┘  └───────────┘  └───────────┘                    │   │
│   │       │                │                │                        │   │
│   │       └────────────────┼────────────────┘                        │   │
│   │                        │                                         │   │
│   │              ┌─────────┴─────────┐                              │   │
│   │              │   ZooKeeper/KRaft │                              │   │
│   │              │   (元数据管理)    │                              │   │
│   │              └───────────────────┘                              │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                      消费者 (Consumer)                           │   │
│   │  - 从 Topic 拉取消息                                              │   │
│   │  - 提交偏移量                                                     │   │
│   │  - 消费者组协调                                                   │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Broker 角色

```java
/**
 * Kafka Broker 核心组件
 */
public class BrokerComponents {

    /**
     * 1. 请求处理器 (Request Handler)
     * - 处理生产者的发送请求
     * - 处理消费者的拉取请求
     * - 处理管理请求 (创建 Topic、分区分配等)
     */
    public static class RequestHandler {
        // 网络线程: 接收请求
        // IO 线程: 处理请求
    }

    /**
     * 2. 日志管理器 (Log Manager)
     * - 管理消息存储
     * - 处理日志段 (Log Segment)
     * - 执行日志清理策略
     */
    public static class LogManager {
        // Log: Topic + Partition 的物理存储
        // LogSegment: 日志文件 + 索引文件
    }

    /**
     * 3. 副本管理器 (Replica Manager)
     * - 管理分区副本
     * - 处理 Leader 选举
     * - 同步副本数据
     */
    public static class ReplicaManager {
        // ISR (In-Sync Replicas): 同步副本集合
        // AR (Assigned Replicas): 所有副本集合
    }

    /**
     * 4. 控制器 (Controller)
     * - ZooKeeper/KRaft 中的 Leader 节点
     * - 负责分区 Leader 选举
     * - 监控 Broker 上下线
     */
    public static class Controller {
        // 分区状态管理
        // Broker 注册/注销
        // 分区分配策略
    }
}
```

---

## 数据模型

```
┌─────────────────────────────────────────────────────────────────┐
│                      Kafka 数据模型                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Topic (主题)                                                   │
│  ├── Partition 0 (分区 0)                                       │
│  │   ├── Segment 1 (日志段)                                      │
│  │   │   ├── 00000000000000000000.log     (消息数据)             │
│  │   │   ├── 00000000000000000000.index   (偏移量索引)           │
│  │   │   └── 00000000000000000000.timeindex (时间索引)           │
│  │   └── Segment 2                                              │
│  │       ├── ...                                                │
│  └── Partition 1                                                │
│       └── ...                                                   │
│                                                                 │
│  Message (消息)                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Offset  │  Key  │  Value  │  Timestamp  │  Headers     │   │
│  │  (8字节) │  (可选)│  (数据) │  (8字节)    │  (元数据)    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 消息结构

```java
/**
 * Kafka 消息结构详解
 */
public class MessageStructure {

    /**
     * 消息字段说明
     *
     * | 字段       | 类型    | 大小    | 说明                                    |
     * |------------|---------|---------|-----------------------------------------|
     * | CRC32      | int     | 4 字节  | 消息校验和 (用于检测损坏)                 |
     * | Magic      | byte    | 1 字节  | 消息版本号 (当前为 2)                     |
     * | Attributes | byte    | 1 字节  | 压缩类型、Timestamp 类型等               |
     * | Timestamp  | long    | 8 字节  | 消息时间戳 (版本 2+)                     |
     * | Key Length | int     | 4 字节  | Key 长度 (-1 表示无 Key)                 |
     * | Key        | bytes   | 变长    | 消息键 (用于分区选择)                     |
     * | Value Length| int    | 4 字节  | Value 长度 (-1 表示删除消息)             |
     * | Value      | bytes   | 变长    | 消息内容                                 |
     */
    public static class MessageFormat {
        private long crc32;           // 校验和
        private byte magic;           // 版本号
        private byte attributes;      // 属性 (压缩类型等)
        private long timestamp;       // 时间戳
        private byte[] key;           // 分区键 (可选)
        private byte[] value;         // 消息内容
    }

    /**
     * 偏移量 (Offset)
     *
     * - 含义: 消息在分区中的唯一序号
     * - 特点: 递增、持久化、由 Kafka 自动管理
     * - 作用: 消费者通过 Offset 定位消息
     *
     * 示例:
     * Partition 0: [Offset: 0] [Offset: 1] [Offset: 2] [Offset: 3] ...
     *                 message1   message2   message3   message4
     */
    public static class OffsetExample {
        // 消费者位置
        long currentPosition = 0;  // 已消费位置

        // 下一条要消费的消息
        long nextOffset = 5;       // 下一条 Offset
    }
}
```

---

## 核心术语

| 术语 | 英文 | 说明 | 重要性 |
|------|------|------|--------|
| **Topic** | 主题 | 消息分类的逻辑容器 | ⭐⭐⭐ |
| **Partition** | 分区 | Topic 的物理分片 | ⭐⭐⭐ |
| **Replica** | 副本 | 分区的冗余备份 | ⭐⭐⭐ |
| **Leader** | Leader 副本 | 负责读写操作的副本 | ⭐⭐⭐ |
| **Follower** | Follower 副本 | 同步 Leader 数据 | ⭐⭐ |
| **ISR** | In-Sync Replicas | 同步副本集合 | ⭐⭐⭐ |
| **Consumer Group** | 消费者组 | 共同消费 Topic 的消费者 | ⭐⭐⭐ |
| **Offset** | 偏移量 | 消息在分区中的位置 | ⭐⭐⭐ |
| **Producer** | 生产者 | 发送消息的客户端 | ⭐⭐⭐ |
| **Consumer** | 消费者 | 接收消息的客户端 | ⭐⭐⭐ |
| **Broker** | 代理 | Kafka 服务节点 | ⭐⭐⭐ |
| **Controller** | 控制器 | 集群管理组件 | ⭐⭐ |

### 术语关系图

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka 术语关系图                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Cluster (集群)                                                 │
│  └── Brokers (多个 Broker)                                      │
│      └── Broker 1                                               │
│          └── Topics (多个 Topic)                                │
│              └── Topic: orders                                  │
│                  └── Partitions (多个 Partition)                 │
│                      └── Partition 0                            │
│                          └── Replicas (副本)                     │
│                              ├── Leader (读写)                   │
│                              └── Followers (同步)                │
│                                                                 │
│  Consumer Group (消费者组)                                      │
│  └── Consumers (多个 Consumer)                                  │
│      └── Consumer 1 → Partition 0                               │
│      └── Consumer 2 → Partition 1                               │
│      └── Consumer 3 → Partition 2                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 分区与副本

### 分区策略

```java
/**
 * Kafka 分区策略详解
 */
public class PartitionStrategy {

    /**
     * 默认分区器: DefaultPartitioner
     *
     * 分区逻辑:
     * 1. 如果指定了 Key: 使用 Key 的哈希值选择分区
     *    hash(key) % numPartitions
     *
     * 2. 如果未指定 Key: 使用粘性分区策略
     *    - 消息先发送到同一个分区
     *    - 累积到 batch.size 或 linger.ms 后再发送
     *    - 然后切换到下一个分区
     *
     * 优势:
     * - 有 Key 时保证相同 Key 的消息有序
     * - 无 Key 时提高批量发送效率
     */
    public static class DefaultPartitionerExample {

        // 场景1: 指定 Key (保证同一用户消息有序)
        ProducerRecord<String, String> record1 =
            new ProducerRecord<>("orders", "user123", "order_1");

        ProducerRecord<String, String> record2 =
            new ProducerRecord<>("orders", "user123", "order_2");
        // Both go to the same partition (e.g., partition 5)

        // 场景2: 不指定 Key (负载均衡)
        ProducerRecord<String, String> record3 =
            new ProducerRecord<>("orders", null, "order_3");
        // Goes to a random partition (sticky)
    }

    /**
     * 自定义分区器示例
     *
     * 使用场景:
     * - 业务需要特定的分区策略
     * - 需要灰度发布 (某些用户走特定分区)
     * - 需要按区域/业务线分区
     */
    public static class CustomPartitioner implements Partitioner {

        private final ConcurrentMap<String, AtomicInteger> countMap =
            new ConcurrentHashMap<>();

        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                             Object value, Cluster cluster) {
            // 策略1: 按区域分区
            if (key instanceof String && ((String) key).startsWith("CN-")) {
                return 0;  // 中国用户到分区 0
            } else if (((String) key).startsWith("US-")) {
                return 1;  // 美国用户到分区 1
            }

            // 策略2: 轮询分区
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            int numPartitions = partitions.size();

            // 使用 Key 哈希
            if (key != null) {
                return Math.abs(Objects.hash(key)) % numPartitions;
            }

            // 粘性分区 (记录当前分区)
            return currentPartition.getAndUpdate(
                current -> (current + 1) % numPartitions
            );
        }

        private final AtomicInteger currentPartition = new AtomicInteger(0);
    }
}
```

### 副本机制

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka 副本机制                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Topic: orders (3 分区, 复制因子: 3)                            │
│                                                                 │
│  Partition 0                    Partition 1                    │
│  ┌───────────────────┐        ┌───────────────────┐           │
│  │ Broker 1 (Leader) │        │ Broker 2 (Leader) │           │
│  │ Offset: 0-999     │        │ Offset: 0-999     │           │
│  └─────────┬─────────┘        └─────────┬─────────┘           │
│            │ Sync                      │ Sync                  │
│  ┌─────────┴─────────┐        ┌─────────┴─────────┐           │
│  │ Broker 2          │        │ Broker 3          │           │
│  │ Offset: 0-999     │        │ Offset: 0-999     │           │
│  └─────────┬─────────┘        └─────────┬─────────┘           │
│            │ Sync                      │ Sync                  │
│  ┌─────────┴─────────┐        ┌─────────┴─────────┐           │
│  │ Broker 3          │        │ Broker 1          │           │
│  │ Offset: 0-999     │        │ Offset: 0-999     │           │
│  └───────────────────┘        └───────────────────┘           │
│                                                                 │
│  ISR (In-Sync Replicas):                                       │
│  - Broker 1, Broker 2, Broker 3 都在 ISR 中                    │
│  - 说明所有副本都与 Leader 保持同步                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```java
/**
 * Kafka 副本配置详解
 */
public class ReplicationConfig {

    /**
     * 复制因子 (Replication Factor)
     *
     * 配置建议:
     * - 开发环境: 1 (单副本)
     * - 测试环境: 2 (双副本)
     * - 生产环境: 3 (三副本)
     *
     * 设置方式:
     * 1. Topic 创建时指定
     * 2. Broker 默认配置
     * 3. Topic 级别覆盖
     */
    public static void replicationFactorConfig() {
        // 方式1: 创建 Topic 时指定
        // kafka-topics.sh --create --topic orders \
        //     --partitions 3 \
        //     --replication-factor 3 \
        //     --bootstrap-server localhost:9092

        // 方式2: Broker 默认配置 (server.properties)
        // default.replication.factor = 3

        // 方式3: 修改现有 Topic
        // kafka-reassign-partitions.sh --reassignment-json-file reassign.json
    }

    /**
     * ISR (In-Sync Replicas) 配置
     *
     * ISR 定义:
     * - 与 Leader 保持同步的副本集合
     * - 同步标准: replica.lag.time.max.ms 内有拉取数据
     *
     * 关键配置:
     */
    public static class ISRConfig {
        // 副本同步最大延迟时间 (默认 10 秒)
        // replica.lag.time.max.ms = 10000

        // 副本落后多少消息时剔除出 ISR
        // replica.lag.max.messages = 1000

        // Leader 选举时允许的最小 ISR 数量
        // min.insync.replicas = 2
    }

    /**
     * ACK 配置 (生产者确认机制)
     *
     * | 配置值 | 说明                          | 可靠性   | 性能   |
     * |--------|-------------------------------|----------|--------|
     * | 0      | 不等待确认                    | 最低     | 最高   |
     * | 1      | 仅等待 Leader 确认            | 中等     | 中等   |
     * | all    | 等待所有 ISR 确认             | 最高     | 最低   |
     */
    public static void acksConfig(Producer<String, String> producer) {
        // 最高可靠性 (推荐生产环境)
        producer.send(new ProducerRecord<>("topic", "message"),
            (metadata, exception) -> {
                if (exception != null) {
                    // 发送失败处理
                } else {
                    // 发送成功
                    System.out.println("Offset: " + metadata.offset());
                }
            });
    }
}
```

---

## 消费者组

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka 消费者组机制                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Topic: orders (3 分区)                                         │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Partition 0  │ Partition 1  │ Partition 2               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Consumer Group: order-service                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Consumer 1 → Partition 0                                 │   │
│  │ Consumer 2 → Partition 1                                 │   │
│  │ Consumer 3 → Partition 2                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Consumer Group: analytics-service                              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Consumer A → Partition 0 (消费所有分区)                   │   │
│  │ Consumer B → Partition 1                                 │   │
│  │ Consumer C → Partition 2                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  规则:                                                          │
│  - 分区数 = 消费者数时: 每个消费者一个分区                        │   │
│  - 分区数 > 消费者数时: 部分消费者多分区                          │   │
│  - 分区数 < 消费者数时: 部分消费者空闲                            │   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

```java
/**
 * Kafka 消费者组详解
 */
public class ConsumerGroupExample {

    /**
     * 消费者组特性
     *
     * 1. 消息分发:
     *    - 同一个分区的消息只被组内一个消费者消费
     *    - 不同分区可被不同消费者并行消费
     *
     * 2. 偏移量管理:
     *    - 组内共享偏移量位置
     *    - 自动或手动提交
     *    - 支持从指定 Offset 消费
     *
     * 3. Rebalance:
     *    - 消费者加入/离开时触发
     *    - 重新分配分区
     *    - 分配策略: Range, RoundRobin, Sticky
     */
    public static void consumerGroupExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "order-service-group");  // 消费者组 ID
        props.put("auto.offset.reset", "earliest");    // 首次消费位置

        KafkaConsumer<String, String> consumer =
            new KafkaConsumer<>(props);

        // 订阅 Topic
        consumer.subscribe(Arrays.asList("orders", "payments"));

        // 消费消息
        while (true) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Partition: %d, Offset: %d, Key: %s%n",
                    record.partition(),
                    record.offset(),
                    record.key());
            }
        }
    }

    /**
     * 分配策略 (Partition Assignment Strategy)
     *
     * | 策略                      | 说明                          |
     * |---------------------------|-------------------------------|
     * | Range                     | 按 Topic 逐个分配 (默认)       |
     * | RoundRobin                | 所有 Topic 分区轮询分配        |
     * | StickyAssignor            | 最小化分区移动                 |
     * | CooperativeStickyAssignor | 无感知 Rebalance (推荐)        |
     */
    public static void assignmentStrategy(Properties props) {
        // 推荐使用 CooperativeStickyAssignor
        props.put("partition.assignment.strategy",
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
    }

    /**
     * Rebalance 监听器
     *
     * 用途:
     * - Rebalance 前保存处理进度
     * - Rebalance 后释放资源
     */
    public static void rebalanceListenerExample() {
        KafkaConsumer<String, String> consumer = ...;

        consumer.subscribe(Arrays.asList("topic"),
            new ConsumerRebalanceListener() {

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Rebalance 前触发
                    // 保存当前处理进度
                    for (TopicPartition partition : partitions) {
                        saveOffset(consumer.position(partition));
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    // Rebalance 后触发
                    // 可以从保存的 Offset 继续消费
                    for (TopicPartition partition : partitions) {
                        consumer.seek(partition, loadOffset(partition));
                    }
                }
            });
    }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Kafka Producer 指南](02-producer.md) | 生产者 API 和配置详解 |
| [Kafka Consumer 指南](03-consumer.md) | 消费者 API 和配置详解 |
| [Kafka Streams 指南](04-streams.md) | 流处理 API 详解 |
| [Kafka 运维指南](05-operations.md) | 集群部署和运维 |
| [Kafka 故障排查](06-troubleshooting.md) | 常见问题和解决方案 |
