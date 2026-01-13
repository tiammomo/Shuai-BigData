# Kafka Consumer 详解

## 目录

- [消费者核心概念](#消费者核心概念)
- [基础消费者实现](#基础消费者实现)
- [消费者配置详解](#消费者配置详解)
- [消费者组管理](#消费者组管理)
- [偏移量管理](#偏移量管理)
- [反序列化器](#反序列化器)
- [监听器](#监听器)
- [最佳实践](#最佳实践)

---

## 消费者核心概念

### 消费者角色

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka Consumer 角色                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  消费者职责:                                                     │
│  1. 订阅 Topic                                                  │
│  2. 从分区拉取消息                                               │
│  3. 反序列化消息                                                 │
│  4. 处理消息                                                     │
│  5. 提交偏移量                                                   │
│                                                                 │
│  消息消费流程:                                                   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Kafka Broker                          │   │
│  │              (Partition Leader)                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│                              ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  消费者 (Consumer)                                        │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐    │   │
│  │  │ poll()  │-->│ 反序列化 │-->│ 处理消息 │-->│ 提交    │    │   │
│  │  │         │  │         │  │         │  │ Offset  │    │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 消费者与消费者组

```
┌─────────────────────────────────────────────────────────────────┐
│                  消费者组与分区关系                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Topic: orders (4 分区)                                         │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐     │
│  │ Partition 0 │ Partition 1 │ Partition 2 │ Partition 3 │     │
│  └─────────────┴─────────────┴─────────────┴─────────────┘     │
│                                                                 │
│  Consumer Group: order-service (4 消费者)                       │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐     │
│  │ Consumer 1  │ Consumer 2  │ Consumer 3  │ Consumer 4  │     │
│  │ --> P0      │ --> P1      │ --> P2      │ --> P3      │     │
│  └─────────────┴─────────────┴─────────────┴─────────────┘     │
│  规则: 分区数 = 消费者数时，一一对应                              │
│                                                                 │
│  Consumer Group: analytics (2 消费者)                            │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐     │
│  │ Consumer A  │ Consumer B  │             │             │     │
│  │ --> P0, P2  │ --> P1, P3  │             │             │     │
│  └─────────────┴─────────────┴─────────────┴─────────────┘     │
│  规则: 分区数 > 消费者数时，负载均衡                              │
│                                                                 │
│  Consumer Group: backup (1 消费者)                               │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐     │
│  │ Consumer X  │             │             │             │     │
│  │ --> P0-P3   │             │             │             │     │
│  └─────────────┴─────────────┴─────────────┴─────────────┘     │
│  规则: 分区数 < 消费者数时，部分消费者空闲                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 基础消费者实现

### 同步消费

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Kafka 消费者 - 同步消费示例
 *
 * 特点:
 * - poll() 拉取消息
 * - 手动提交偏移量
 * - 处理消息后提交
 */
public class SyncConsumerExample {

    /**
     * 创建消费者配置
     *
     * 必要配置项:
     * - bootstrap.servers: Kafka 集群地址
     * - group.id: 消费者组 ID
     * - key.deserializer: Key 反序列化器
     * - value.deserializer: Value 反序列化器
     */
    public static Properties createConsumerConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        // Kafka 集群地址
        props.put("bootstrap.servers", "localhost:9092");

        // 消费者组 ID (必需)
        // 相同 group.id 的消费者组成消费者组，共同消费 Topic
        props.put("group.id", "order-service-group");

        // Key 反序列化器
        props.put("key.deserializer", StringDeserializer.class.getName());

        // Value 反序列化器
        props.put("value.deserializer", StringDeserializer.class.getName());

        // ============ 自动提交配置 ============
        // 是否开启自动提交偏移量
        // 自动提交: poll() 消息后定期提交
        props.put("enable.auto.commit", false);  // 手动提交

        // 自动提交间隔 (毫秒) - 仅 enable.auto.commit=true 时有效
        props.put("auto.commit.interval.ms", 5000);

        // ============ 消费位置配置 ============
        // 首次消费位置
        // earliest: 从最早位置开始消费
        // latest: 从最新位置开始消费
        // none: 无消费位置则报错
        props.put("auto.offset.reset", "earliest");

        return props;
    }

    /**
     * 同步消费消息
     *
     * 执行流程:
     * 1. 创建 KafkaConsumer
     * 2. 订阅 Topic
     * 3. 循环 poll() 消息
     * 4. 处理消息
     * 5. 手动提交偏移量
     */
    public static void consumeSync(KafkaConsumer<String, String> consumer) {
        try {
            // 订阅 Topic (支持正则表达式)
            consumer.subscribe(Arrays.asList("orders", "payments"));

            System.out.println("开始消费消息...");

            while (true) {
                // ============ 拉取消息 ============
                // poll() 返回指定时间内的消息批次
                // Duration: 等待消息的最大时间
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

                // ============ 处理消息 ============
                if (!records.isEmpty()) {
                    System.out.printf("收到 %d 条消息%n", records.count());

                    for (ConsumerRecord<String, String> record : records) {
                        processMessage(record);
                    }

                    // ============ 提交偏移量 ============
                    // 手动提交 - 确保消息处理完成后提交
                    consumer.commitSync();  // 同步提交

                    System.out.println("偏移量提交完成");
                }
            }

        } catch (Exception e) {
            System.err.println("消费异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭消费者
            consumer.close();
        }
    }

    /**
     * 处理单条消息
     */
    private static void processMessage(ConsumerRecord<String, String> record) {
        System.out.println("====================");
        System.out.println("Topic: " + record.topic());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        System.out.println("Timestamp: " + record.timestamp());
        System.out.println("TimestampType: " + record.timestampType());
        System.out.println("====================");
    }

    /**
     * 指定分区消费
     *
     * 适用场景:
     * - 只想消费特定分区
     * - 手动分配分区
     */
    public static void consumeSpecificPartitions(KafkaConsumer<String, String> consumer) {
        // 指定消费分区 0 和 1
        consumer.assign(Arrays.asList(
            new TopicPartition("orders", 0),
            new TopicPartition("orders", 1)
        ));

        // 从指定偏移量开始消费
        consumer.seek(new TopicPartition("orders", 0), 100L);
        consumer.seek(new TopicPartition("orders", 1), 200L);

        // 循环消费
        while (true) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Partition: %d, Offset: %d, Value: %s%n",
                    record.partition(), record.offset(), record.value());
            }

            consumer.commitSync();
        }
    }

    public static void main(String[] args) {
        Properties props = createConsumerConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费消息
        consumeSync(consumer);
    }
}
```

### 异步消费

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.util.*;

/**
 * Kafka 消费者 - 异步消费与手动提交
 *
 * 特点:
 * - 支持异步提交
 * - 支持回调处理提交结果
 * - 更细粒度的偏移量控制
 */
public class AsyncConsumerExample {

    /**
     * 异步提交偏移量
     *
     * 优点:
     * - 不阻塞消费者
     * - 提高吞吐量
     * - 支持批量提交
     */
    public static void asyncCommit(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Arrays.asList("orders"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // 处理消息
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                // ============ 异步提交 ============
                // commitAsync() 异步提交，不阻塞
                // 可以指定回调处理提交结果
                consumer.commitAsync((offsets, exception) -> {
                    if (exception != null) {
                        // 提交失败处理
                        System.err.println("偏移量提交失败: " + exception.getMessage());

                        // 可选: 记录日志或重试
                        handleCommitFailure(offsets, exception);
                    } else {
                        // 提交成功
                        System.out.printf("偏移量提交成功: %s%n", offsets);
                    }
                });
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 批量处理 + 批量提交
     *
     * 适用场景:
     * - 高吞吐量场景
     * - 可以容忍少量重复消息
     */
    public static void batchProcessing(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Arrays.asList("orders"));

        // 批量处理配置
        final int batchSize = 100;
        final List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // 收集消息
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);

                    // 达到批次大小，处理并提交
                    if (buffer.size() >= batchSize) {
                        processBatch(buffer);
                        consumer.commitSync();  // 同步提交确保成功
                        buffer.clear();
                    }
                }

                // 处理剩余消息
                if (!buffer.isEmpty()) {
                    processBatch(buffer);
                    consumer.commitSync();
                    buffer.clear();
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 处理批次消息
     */
    private static void processBatch(List<ConsumerRecord<String, String>> batch) {
        System.out.printf("处理批次消息: %d 条%n", batch.size());

        // 批次处理逻辑
        for (ConsumerRecord<String, String> record : batch) {
            // 处理每条消息
        }
    }

    /**
     * 按分区提交偏移量
     *
     * 适用场景:
     * - 处理完一个分区的消息后立即提交
     * - 更细粒度的提交控制
     */
    public static void perPartitionCommit(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Arrays.asList("orders"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // 按分区处理
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit =
                    new HashMap<>();

                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords =
                        records.records(partition);

                    // 处理分区消息
                    long lastOffset = processPartition(partitionRecords);

                    // 记录要提交的偏移量 (下一条消息的偏移量)
                    offsetsToCommit.put(
                        partition,
                        new OffsetAndMetadata(lastOffset + 1)
                    );
                }

                // 批量提交
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 处理单个分区的消息
     *
     * @return 最后一条消息的偏移量
     */
    private static long processPartition(List<ConsumerRecord<String, String>> records) {
        long lastOffset = -1;

        for (ConsumerRecord<String, String> record : records) {
            System.out.printf("Partition: %d, Offset: %d%n",
                record.partition(), record.offset());
            lastOffset = record.offset();
        }

        return lastOffset;
    }

    /**
     * 处理记录
     */
    private static void processRecord(ConsumerRecord<String, String> record) {
        System.out.printf("收到消息: Topic=%s, Partition=%d, Offset=%d%n",
            record.topic(), record.partition(), record.offset());
    }

    /**
     * 处理提交失败
     */
    private static void handleCommitFailure(Map<TopicPartition, OffsetAndMetadata> offsets,
                                             Exception exception) {
        // 记录失败日志
        System.err.println("提交失败详情: " + offsets);

        // 可选: 重试或降级处理
        // 1. 重试提交
        // 2. 降级为同步提交
        // 3. 记录到死信队列
    }
}
```

---

## 消费者配置详解

### 配置分类

```
┌─────────────────────────────────────────────────────────────────┐
│                  Kafka Consumer 配置分类                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   基础配置       │  │   消费者组配置   │  │   消费位置配置   │  │
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤  │
│  │ bootstrap.servers│  │  group.id      │  │ auto.offset.reset│  │
│  │ key.deserializer│  │  group.instance│  │  enable.auto.com-│  │
│  │ value.deserializer│ │  session.timeout│ │  mit            │  │
│  │                 │  │  max.poll.inter │  │  auto.commit.in-│  │
│  │                 │  │  val.ms         │  │  terval.ms      │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   分区分配配置   │  │   拉取配置       │  │   心跳配置       │  │
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤  │
│  │ partition.assign│  │ fetch.min.bytes │  │ heartbeat.inter-│  │
│  │ ment.strategy   │  │ fetch.max.wait. │  │ val.ms          │  │
│  │                 │  │ ms              │  │                 │  │
│  │                 │  │ max.partition.  │  │                 │  │
│  │                 │  │ fetch.bytes     │  │                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 完整配置示例

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;

/**
 * Kafka 消费者完整配置详解
 */
public class ConsumerConfigExample {

    /**
     * 基础消费配置
     */
    public static Properties basicConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        // Kafka 集群地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 消费者组 ID (必需)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");

        // Key 反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());

        // Value 反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());

        return props;
    }

    /**
     * 自动提交配置 (简单场景)
     *
     * 特点:
     * - 自动定期提交偏移量
     * - 可能产生重复消费
     * - 配置简单
     */
    public static Properties autoCommitConfig() {
        Properties props = basicConfig();

        // 开启自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        // 自动提交间隔 (毫秒) - 默认 5000
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 5000);

        // 首次消费位置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    /**
     * 手动提交配置 (生产环境推荐)
     *
     * 特点:
     * - 手动控制偏移量提交
     * - 消息处理后才提交
     * - 避免重复消费
     */
    public static Properties manualCommitConfig() {
        Properties props = basicConfig();

        // 关闭自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 首次消费位置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    /**
     * 高吞吐量配置
     *
     * 特点:
     * - 增大拉取大小
     * - 减少拉取频率
     * - 支持批量处理
     */
    public static Properties highThroughputConfig() {
        Properties props = basicConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "throughput-group");

        // 最小拉取大小 (字节) - 默认 1
        // 累积到这么多字节才返回
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 100);  // 100KB

        // 最大等待时间 (毫秒) - 默认 500
        // 等待这么多时间后返回，即使没达到 FETCH_MIN_BYTES
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // 单次拉取最大数据量 (字节) - 默认 50MB
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
            1024 * 1024);  // 1MB

        // 每次 poll 最大消息数 - 默认 500
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        return props;
    }

    /**
     * 低延迟配置
     *
     * 特点:
     * - 快速拉取
     * - 小批次返回
     */
    public static Properties lowLatencyConfig() {
        Properties props = basicConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "latency-group");

        // 最小拉取大小
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1);

        // 短等待时间
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);

        // 每次 poll 少量消息
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return props;
    }

    /**
     * 消费者组会话配置
     *
     * 用于检测消费者是否存活
     */
    public static Properties sessionConfig() {
        Properties props = basicConfig();

        // 会话超时时间 (毫秒) - 默认 10000 (10秒)
        // 消费者在此时间内未发送心跳，认为死亡
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);

        // 心跳间隔时间 (毫秒) - 默认 3000
        // 心跳线程发送心跳的间隔
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);

        // 最大拉取间隔 (毫秒) - 默认 300000 (5分钟)
        // 消费者调用 poll() 的最大间隔
        // 超过此时间未调用 poll()，认为消费者死亡
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        return props;
    }

    /**
     * 分区分配策略配置
     */
    public static Properties partitionAssignmentConfig() {
        Properties props = basicConfig();

        // 分区分配策略
        // Range: 按 Topic 逐个分配 (默认)
        // RoundRobin: 所有 Topic 分区轮询分配
        // StickyAssignor: 最小化分区移动
        // CooperativeStickyAssignor: 无感知 Rebalance (推荐)
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        return props;
    }

    /**
     * 生产环境完整配置
     */
    public static Properties productionConfig() {
        Properties props = new Properties();

        // 基础配置
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "kafka1:9092,kafka2:9092,kafka3:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "production-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());

        // 提交配置
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 拉取配置
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 50);  // 50KB
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 300);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        // 会话配置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        // 分配策略
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        // 消费者实例 ID (静态成员资格)
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "consumer-1");

        return props;
    }
}
```

---

## 消费者组管理

### Rebalance 机制

```java
import org.apache.kafka.clients.consumer.*;
import java.util.Collection;

/**
 * Kafka 消费者 Rebalance 详解
 *
 * Rebalance 触发条件:
 * 1. 消费者加入组
 * 2. 消费者离开组 (主动或被动)
 * 3. Topic 分区数量变化
 * 4. 消费者组订阅的 Topic 变化
 *
 * Rebalance 过程:
 * 1. 通知所有消费者准备 Rebalance
 * 2. 停止消费
 * 3. 重新分配分区
 * 4. 恢复消费
 */
public class RebalanceExample {

    /**
     * Rebalance 监听器
     *
     * 用途:
     * - Rebalance 前保存处理进度
     * - Rebalance 后恢复消费位置
     */
    public static class RebalanceListenerExample {

        private KafkaConsumer<String, String> consumer;
        private Map<TopicPartition, Long> currentOffsets = new java.util.HashMap<>();

        public RebalanceListenerExample(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        /**
         * Rebalance 监听器
         */
        public ConsumerRebalanceListener createListener() {
            return new ConsumerRebalanceListener() {

                /**
                 * Rebalance 开始前触发
                 *
                 * 用途:
                 * - 保存当前处理进度
                 * - 清理资源
                 */
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("Rebalance 前 - 失去分区:");
                    for (TopicPartition partition : partitions) {
                        // 获取当前消费位置
                        long position = consumer.position(partition);
                        currentOffsets.put(partition, position);
                        System.out.printf("  %s: position=%d%n", partition, position);
                    }

                    // 可选: 提交偏移量
                    consumer.commitSync(currentOffsets);

                    // 可选: 清理资源
                    // closeConnections();
                }

                /**
                 * 分区分配完成后触发
                 *
                 * 用途:
                 * - 从保存的位置恢复消费
                 * - 初始化资源
                 */
                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("Rebalance 后 - 获得分区:");
                    for (TopicPartition partition : partitions) {
                        System.out.printf("  %s%n", partition);

                        // 从保存的位置恢复消费
                        if (currentOffsets.containsKey(partition)) {
                            consumer.seek(partition, currentOffsets.get(partition));
                        }
                    }

                    // 可选: 初始化资源
                    // initConnections();
                }
            };
        }

        /**
         * 使用 Rebalance 监听器
         */
        public void subscribeWithListener() {
            ConsumerRebalanceListener listener = createListener();

            // 订阅 Topic 时指定监听器
            consumer.subscribe(Arrays.asList("orders"), listener);
        }
    }

    /**
     * 静态成员资格 (避免不必要的 Rebalance)
     *
     * Kafka 2.4+ 支持
     * 消费者可以配置 group.instance.id
     * 临时离开不会触发 Rebalance
     */
    public static void staticMembershipExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "static-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        // 静态成员资格 ID
        // 消费者重启时使用相同 ID，不会触发 Rebalance
        props.put("group.instance.id", "consumer-1");  // 必须是唯一 ID

        // 最大等待时间 (毫秒)
        // 消费者离开后，在此时间内返回不会触发 Rebalance
        props.put("session.timeout.ms", 30000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("orders"));

        // 消费者可以临时离开 (pause消费) 一段时间
        // 超过 session.timeout.ms 未返回才会触发 Rebalance
    }

    /**
     * 分区分配策略对比
     */
    public static class PartitionAssignmentStrategy {

        /**
         * 分配策略配置
         */
        public static void assignmentStrategy() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "strategy-group");

            // 策略配置
            // 1. RangeAssignor (默认)
            //    - 按 Topic 逐个分配
            //    - 可能导致分配不均
            props.put("partition.assignment.strategy",
                "org.apache.kafka.clients.consumer.RangeAssignor");

            // 2. RoundRobinAssignor
            //    - 所有 Topic 分区轮询分配
            //    - 分配更均匀
            props.put("partition.assignment.strategy",
                "org.apache.kafka.clients.consumer.RoundRobinAssignor");

            // 3. StickyAssignor
            //    - 最小化分区移动
            //    - 保持分配稳定
            props.put("partition.assignment.strategy",
                "org.apache.kafka.clients.consumer.StickyAssignor");

            // 4. CooperativeStickyAssignor (推荐)
            //    - 无感知 Rebalance
            //    - 不会暂停消费
            props.put("partition.assignment.strategy",
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

            // 5. 自定义策略
            // 可以实现 PartitionAssignor 接口
        }
    }
}
```

---

## 偏移量管理

### 偏移量概念

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka 偏移量管理                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Partition 结构:                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ 消息序列                                                    │   │
│  │ [0] [1] [2] [3] [4] [5] [6] [7] [8] [9] ...              │   │
│  │   ↑                               ↑                       │   │
│  │  Log Start Offset              High Watermark            │   │
│  │ (最早消息)                      (已同步副本的最大偏移)      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  消费者位置:                                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ [0] [1] [2] [3] [4] [5] [6] [7] [8] [9] ...              │   │
│  │           ↑                  ↑                           │   │
│  │   Committed Offset      Current Position                 │   │
│  │   (已提交位置)           (poll 返回的位置)                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  消费状态:                                                       │
│  - Committed Offset: 已提交的消费位置                            │
│  - Current Position: 当前消费位置 (通常 = committed + 1)         │
│  - Last Stable Offset: 最新可消费位置 (事务提交前)               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 偏移量提交策略

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.util.*;

/**
 * Kafka 偏移量管理详解
 */
public class OffsetManagementExample {

    /**
     * 1. 自动提交
     *
     * 特点:
     * - poll() 消息后在后台线程提交
     * - 可能导致重复消费
     * - 配置简单
     */
    public static void autoCommitExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "auto-commit-group");
        props.put("enable.auto.commit", true);        // 开启自动提交
        props.put("auto.commit.interval.ms", 5000);   // 提交间隔

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        // 自动提交在后台线程进行
        // poll() 返回的消息可能还未提交
        // 如果消费者崩溃，可能重复消费最近的消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                // 处理消息
                // 如果在这里崩溃，消息可能被重复消费
            }
        }
    }

    /**
     * 2. 同步手动提交
     *
     * 特点:
     * - 消息处理完成后提交
     * - 阻塞等待提交完成
     * - 避免重复消费
     */
    public static void syncCommitExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "sync-commit-group");
        props.put("enable.auto.commit", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                processMessage(record);
            }

            // 同步提交 - 阻塞等待
            // 如果提交失败，会抛出异常
            try {
                consumer.commitSync();
                System.out.println("偏移量同步提交成功");
            } catch (CommitFailedException e) {
                System.err.println("提交失败: " + e.getMessage());
                // 处理提交失败
            }
        }
    }

    /**
     * 3. 异步手动提交
     *
     * 特点:
     * - 不阻塞消费者
     * - 提交速度快
     * - 可能丢失最后几次提交
     */
    public static void asyncCommitExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "async-commit-group");
        props.put("enable.auto.commit", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                processMessage(record);
            }

            // 异步提交 - 不阻塞
            consumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    System.err.println("异步提交失败: " + exception.getMessage());
                } else {
                    System.out.println("异步提交成功: " + offsets);
                }
            });
        }
    }

    /**
     * 4. 按分区提交特定偏移量
     *
     * 特点:
     * - 细粒度控制
     * - 可跳过后不想要的消息
     */
    public static void perPartitionCommitExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "per-partition-group");
        props.put("enable.auto.commit", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // 按分区处理
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords =
                    records.records(partition);

                long lastOffset = -1;
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    processRecord(record);
                    lastOffset = record.offset();
                }

                // 提交下一条消息的偏移量
                long nextOffset = lastOffset + 1;
                offsets.put(partition, new OffsetAndMetadata(nextOffset));

                System.out.printf("分区 %d 处理完成，提交偏移量: %d%n",
                    partition.partition(), nextOffset);
            }

            // 批量提交
            consumer.commitSync(offsets);
        }
    }

    /**
     * 5. 从指定偏移量消费
     *
     * 场景:
     * - 重新处理历史数据
     * - 跳过部分消息
     * - 故障恢复
     */
    public static void seekExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "seek-group");
        props.put("enable.auto.commit", false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        // 获取分区信息
        List<TopicPartition> partitions = new ArrayList<>();
        for (TopicPartition partition : consumer.assignment()) {
            partitions.add(partition);
        }

        // 方式1: 消费所有分区的最早消息
        for (TopicPartition partition : partitions) {
            consumer.seekToBeginning(Collections.singletonList(partition));
        }

        // 方式2: 消费所有分区的最新消息
        for (TopicPartition partition : partitions) {
            consumer.seekToEnd(Collections.singletonList(partition));
        }

        // 方式3: 从指定偏移量消费
        consumer.seek(new TopicPartition("orders", 0), 100L);
        consumer.seek(new TopicPartition("orders", 1), 200L);

        // 消费消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
            }
        }
    }

    /**
     * 6. 事务性提交 (Kafka 0.11+)
     *
     * 特点:
     * - 消息处理和偏移量提交在同一事务
     * -  Exactly-Once 语义
     */
    public static void transactionalCommitExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "transactional-group");
        props.put("enable.auto.commit", false);

        // 事务配置
        props.put("isolation.level", "read_committed");  // 只能读到已提交消息
        props.put("transactional.id", "transaction-1");   // 事务 ID

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 初始化事务
        consumer.initTransactions();

        while (true) {
            try {
                // 开始事务
                consumer.beginTransaction();

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }

                // 提交偏移量 (在事务中)
                // 消息处理和偏移量提交原子性
                consumer.sendOffsetsToTransaction(
                    getCurrentOffsets(consumer),
                    "transactional-group"
                );

                // 提交事务
                consumer.commitTransaction();

            } catch (KafkaafkaException e) {
                // 中止事务
                consumer.abortTransaction();
                System.err.println("事务失败: " + e.getMessage());
            }
        }
    }

    private static Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets(
            KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition partition : consumer.assignment()) {
            long position = consumer.position(partition);
            offsets.put(partition, new OffsetAndMetadata(position));
        }
        return offsets;
    }

    private static void processMessage(ConsumerRecord<String, String> record) {
        System.out.printf("处理消息: %s%n", record.value());
    }

    private static void processRecord(ConsumerRecord<String, String> record) {
        System.out.printf("Partition: %d, Offset: %d, Value: %s%n",
            record.partition(), record.offset(), record.value());
    }
}
```

---

## 反序列化器

### 内置反序列化器

```java
import org.apache.kafka.common.serialization.*;
import java.nio.charset.StandardCharsets;

/**
 * Kafka 反序列化器详解
 *
 * 反序列化器作用:
 * - 将字节数组转换为 Java 对象
 * - 从 Broker 接收消息后解码
 *
 * 内置反序列化器:
 * | 反序列化器           | 说明                     |
 * |--------------------|------------------------|
 * | StringDeserializer | UTF-8 字符串            |
 * | IntegerDeserializer| 整数                    |
 * | LongDeserializer   | 长整数                  |
 * | DoubleDeserializer | 双精度浮点数             |
 * | ByteArrayDeserializer | 字节数组              |
 * | ByteBufferDeserializer| ByteBuffer           |
 * | BytesDeserializer  | Kafka Bytes            |
 */
public class DeserializerExample {

    /**
     * String 反序列化器使用
     */
    public static void stringDeserializer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "string-consumer");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        // 接收 JSON 字符串
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(
                java.time.Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                String json = record.value();
                // 解析 JSON
                System.out.println("收到消息: " + json);
            }
        }
    }

    /**
     * 整数/长整数反序列化器
     */
    public static void numberDeserializer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "number-consumer");
        props.put("key.deserializer", LongDeserializer.class.getName());
        props.put("value.deserializer", LongDeserializer.class.getName());

        KafkaConsumer<Long, Long> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("timestamps"));

        while (true) {
            ConsumerRecords<Long, Long> records = consumer.poll(
                java.time.Duration.ofMillis(100));

            for (ConsumerRecord<Long, Long> record : records) {
                Long timestamp = record.value();
                System.out.println("时间戳: " + timestamp);
            }
        }
    }
}

/**
 * 自定义反序列化器
 */
class CustomDeserializerExample {

    /**
     * 订单对象
     */
    public static class Order {
        private long orderId;
        private String userId;
        private double amount;

        // getter/setter
        public Order() {}

        public Order(long orderId, String userId, double amount) {
            this.orderId = orderId;
            this.userId = userId;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return String.format("Order{orderId=%d, userId=%s, amount=%.2f}",
                orderId, userId, amount);
        }
    }

    /**
     * JSON 反序列化器
     */
    public static class JsonDeserializer<T> implements Deserializer<T> {

        private Class<T> targetType;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // 从配置获取目标类型
            String type = (String) configs.get("target.type");
            try {
                targetType = (Class<T>) Class.forName(type);
            } catch (ClassNotFoundException e) {
                throw new DeserializationException("类型转换失败", e);
            }
        }

        @Override
        public T deserialize(String topic, byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                // 使用 Jackson 或 Gson 解析
                // ObjectMapper mapper = new ObjectMapper();
                // return mapper.readValue(data, targetType);

                String json = new String(data, StandardCharsets.UTF_8);
                System.out.println("反序列化: " + json);

                // 简单示例
                if (targetType == Order.class) {
                    return (T) parseOrder(json);
                }

                return null;
            } catch (Exception e) {
                throw new DeserializationException("反序列化失败", e);
            }
        }

        private Order parseOrder(String json) {
            // 简单解析 JSON
            Order order = new Order();
            order.orderId = extractLong(json, "orderId");
            order.userId = extractString(json, "userId");
            order.amount = extractDouble(json, "amount");
            return order;
        }

        private long extractLong(String json, String field) {
            String pattern = "\"" + field + "\":";
            int idx = json.indexOf(pattern);
            if (idx < 0) return 0;
            String value = json.substring(idx + pattern.length());
            value = value.split("[,}]")[0].trim();
            return Long.parseLong(value);
        }

        private String extractString(String json, String field) {
            String pattern = "\"" + field + "\":\"";
            int idx = json.indexOf(pattern);
            if (idx < 0) return null;
            String value = json.substring(idx + pattern.length());
            return value.split("\"")[0];
        }

        private double extractDouble(String json, String field) {
            String pattern = "\"" + field + "\":";
            int idx = json.indexOf(pattern);
            if (idx < 0) return 0;
            String value = json.substring(idx + pattern.length());
            value = value.split("[,}]")[0].trim();
            return Double.parseDouble(value);
        }

        @Override
        public void close() {}
    }

    /**
     * 使用自定义反序列化器
     */
    public static void useCustomDeserializer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "custom-deserializer");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", JsonDeserializer.class.getName());
        props.put("target.type", "com.example.Order");

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        while (true) {
            ConsumerRecords<String, Order> records = consumer.poll(
                java.time.Duration.ofMillis(100));

            for (ConsumerRecord<String, Order> record : records) {
                Order order = record.value();
                System.out.println("收到订单: " + order);
            }
        }
    }
}
```

---

## 监听器

### 消费者监听器

```java
import org.apache.kafka.clients.consumer.*;
import java.util.Collection;

/**
 * Kafka 消费者监听器详解
 */
public class ConsumerListenerExample {

    /**
     * 消费者监听器接口
     *
     * 1. ConsumerRebalanceListener - Rebalance 监听器
     * 2. ConsumerInterceptor - 消费拦截器
     */
    public static class ListenerExamples {

        /**
         * Rebalance 监听器示例
         */
        public static class RebalanceListenerExample {

            private KafkaConsumer<String, String> consumer;
            private Map<String, Long> processedOffsets = new java.util.HashMap<>();

            public RebalanceListenerExample(KafkaConsumer<String, String> consumer) {
                this.consumer = consumer;
            }

            public ConsumerRebalanceListener createListener() {
                return new ConsumerRebalanceListener() {

                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        System.out.println("分区被回收:");
                        for (TopicPartition partition : partitions) {
                            // 获取当前位置
                            long position = consumer.position(partition);

                            // 保存处理进度
                            processedOffsets.put(
                                partition.topic() + "-" + partition.partition(),
                                position
                            );

                            System.out.printf("  %s-%d: position=%d%n",
                                partition.topic(), partition.partition(), position);

                            // 可选: 立即提交
                            consumer.commitSync();
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        System.out.println("分区已分配:");
                        for (TopicPartition partition : partitions) {
                            String key = partition.topic() + "-" + partition.partition();

                            // 从保存的位置恢复
                            if (processedOffsets.containsKey(key)) {
                                long savedOffset = processedOffsets.get(key);
                                consumer.seek(partition, savedOffset);
                                System.out.printf("  %s-%d: 从 %d 恢复%n",
                                    partition.topic(), partition.partition(), savedOffset);
                            } else {
                                System.out.printf("  %s-%d: 新分配%n",
                                    partition.topic(), partition.partition());
                            }
                        }
                    }
                };
            }

            public void subscribe() {
                ConsumerRebalanceListener listener = createListener();
                consumer.subscribe(Arrays.asList("orders"), listener);
            }
        }

        /**
         * 消费拦截器示例
         */
        public static class ConsumptionInterceptor implements ConsumerInterceptor<String, String> {

            private final AtomicLong messageCount = new AtomicLong(0);
            private final AtomicLong errorCount = new AtomicLong(0);

            @Override
            public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
                // 消费前拦截
                messageCount.addAndGet(records.count());
                return records;
            }

            @Override
            public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
                // 提交后回调
                System.out.println("偏移量提交: " + offsets.size() + " 个分区");
            }

            @Override
            public void close() {}

            @Override
            public void configure(Map<String, ?> configs) {}

            public long getMessageCount() { return messageCount.get(); }
            public long getErrorCount() { return errorCount.get(); }
        }

        /**
         * 使用拦截器
         */
        public static void useInterceptor() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("group.id", "interceptor-group");
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());

            // 配置拦截器
            props.put("interceptor.classes",
                "com.example.ConsumptionInterceptor");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList("orders"));
        }
    }

    /**
     * 消费者事件监听器
     */
    public static class ConsumerEventListener {

        /**
         * 消费者状态变更监听
         */
        public static void stateChangeListenerExample() {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(basicConfig());

            // 获取协程
            org.apache.kafka.clients.consumer.internals.ConsumerCoordinator coordinator =
                consumer.coordinator();

            // 监听 Rebalance 完成事件
            // 可以通过覆盖 ConsumerRebalanceListener 实现
        }
    }
}
```

---

## 最佳实践

### 生产环境配置

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Properties;

/**
 * Kafka Consumer 最佳实践
 */
public class ConsumerBestPractices {

    /**
     * 生产环境推荐配置
     */
    public static Properties productionConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "kafka1:9092,kafka2:9092,kafka3:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "production-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());

        // ============ 提交配置 ============
        // 手动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ============ 拉取配置 ============
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 50);  // 50KB
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 300);      // 300ms
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);       // 每次 500 条

        // ============ 会话配置 ============
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        // ============ 分配策略 ============
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        return props;
    }

    /**
     * 优雅关闭消费者
     */
    public static void gracefulShutdown() {
        Properties props = productionConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        // 使用 Runtime 钩子处理关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("收到关闭信号...");

            // 1. 停止轮询
            // 可以设置一个标志位
            consumer.wakeup();  // 唤醒 poll()，抛出 WakeupException

            // 2. 等待当前处理完成
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // 3. 提交最后偏移量
            consumer.close();
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    // 处理消息
                    process(record);

                    // 定期提交
                    // 可以使用计数器，每处理 N 条提交一次
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // 正常关闭异常，忽略
            System.out.println("消费者已关闭");
        }
    }

    /**
     * 消息处理最佳实践
     */
    public static void messageProcessingBestPractice() {
        Properties props = productionConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        while (true) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(100));

            if (records.isEmpty()) {
                continue;
            }

            // 1. 按分区批量处理
            for (TopicPartition partition : records.partitions()) {
                processPartition(records.records(partition));
            }

            // 2. 提交偏移量
            consumer.commitAsync();  // 异步提交
        }
    }

    private static void processPartition(
            java.util.List<ConsumerRecord<String, String>> records) {
        long lastOffset = -1;

        for (ConsumerRecord<String, String> record : records) {
            try {
                // 2.1 处理消息
                process(record);

                // 2.2 记录最后偏移量
                lastOffset = record.offset();

            } catch (Exception e) {
                // 2.3 处理失败 - 记录到死信队列
                handleFailure(record, e);
            }
        }

        // 2.4 提交该分区的偏移量
        if (lastOffset >= 0) {
            System.out.printf("分区 %d 处理完成，最后偏移量: %d%n",
                records.get(0).partition(), lastOffset);
        }
    }

    private static void process(ConsumerRecord<String, String> record) {
        // 业务处理逻辑
        System.out.println("处理消息: " + record.value());
    }

    private static void handleFailure(ConsumerRecord<String, String> record,
                                       Exception e) {
        // 发送到死信队列
        System.err.printf("消息处理失败: %s, error: %s%n",
            record.value(), e.getMessage());
    }

    /**
     * 监控指标
     */
    public static void monitoringMetrics() {
        Properties props = productionConfig();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("orders"));

        // 获取指标
        java.util.Map<org.apache.kafka.common.MetricName, ? extends org.apache.kafka.common.metrics.Metric>
            metrics = consumer.metrics();

        // 关键指标
        // consumer-fetch-rate: 拉取速率
        // consumer-fetch-size-avg: 平均拉取大小
        // poll-rate: poll 调用频率
        // poll-records-avg: 每次 poll 返回消息数

        // 示例
        double fetchRate = metrics.get(
            new org.apache.kafka.common.MetricName("fetch-rate",
                "consumer-fetch-manager", "消费拉取速率", new java.util.HashMap<>())
        ).value();

        System.out.println("拉取速率: " + fetchRate + " 次/秒");

        consumer.close();
    }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Kafka 架构详解](01-architecture.md) | Kafka 核心架构和概念 |
| [Kafka Producer 指南](02-producer.md) | 生产者 API 和配置详解 |
| [Kafka Streams 指南](04-streams.md) | 流处理 API 详解 |
| [Kafka 运维指南](05-operations.md) | 集群部署和运维 |
| [Kafka 故障排查](06-troubleshooting.md) | 常见问题和解决方案 |
