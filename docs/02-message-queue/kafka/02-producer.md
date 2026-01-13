# Kafka Producer 详解

## 目录

- [生产者核心概念](#生产者核心概念)
- [基础生产者实现](#基础生产者实现)
- [生产者配置详解](#生产者配置详解)
- [分区策略](#分区策略)
- [序列化器](#序列化器)
- [压缩机制](#压缩机制)
- [拦截器](#拦截器)
- [最佳实践](#最佳实践)

---

## 生产者核心概念

### 生产者角色

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kafka Producer 角色                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  生产者职责:                                                     │
│  1. 创建消息记录 (ProducerRecord)                                │
│  2. 序列化消息 (Key/Value → 字节数组)                            │
│  3. 计算目标分区                                                 │
│  4. 批量发送消息                                                 │
│  5. 压缩消息 (可选)                                              │
│  6. 发送确认 (ACK)                                               │
│                                                                 │
│  消息发送流程:                                                   │
│                                                                 │
│  ProducerRecord                                                 │
│       │                                                        │
│       ▼                                                        │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │
│  │  序列化器    │ --> │  分区器     │ --> │  缓冲区     │       │
│  │ Serializer  │     │ Partitioner │     │  Buffer     │       │
│  └─────────────┘     └─────────────┘     └──────┬──────┘       │
│                                                  │              │
│                                                  ▼              │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │
│  │  压缩器     │ <-- │   发送线程   │ <-- │  批次发送   │       │
│  │ Compressor  │     │  Sender     │     │  Batching   │       │
│  └─────────────┘     └─────────────┘     └──────┬──────┘       │
│                                                  │              │
│                                                  ▼              │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Kafka Broker                          │   │
│  │              (Partition Leader)                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### ProducerRecord 结构

```java
/**
 * ProducerRecord - Kafka 消息记录
 *
 * 核心字段:
 * | 字段      | 类型                | 说明                           |
 * |-----------|---------------------|--------------------------------|
 * | topic     | String              | 目标 Topic                     |
 * | partition | Integer (可选)      | 目标分区 (指定则忽略分区器)      |
 * | timestamp | Long (可选)         | 消息时间戳                     |
 * | key       | K (可选)            | 分区键 (用于分区选择)           |
 * | value     | V                   | 消息内容                       |
 * | headers   | Iterable<Header>    | 消息头 (元数据)                |
 */
public class ProducerRecord<K, V> {
    private final String topic;          // 目标 Topic 名称
    private final Integer partition;     // 目标分区 (可为 null)
    private final long timestamp;        // 消息时间戳
    private final K key;                 // 分区键
    private final V value;               // 消息内容
    private final Iterable<Header> headers;  // 消息头

    // 构造方法变体
    public ProducerRecord(String topic, V value) { ... }
    public ProducerRecord(String topic, K key, V value) { ... }
    public ProducerRecord(String topic, Integer partition, K key, V value) { ... }
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) { ... }
}
```

---

## 基础生产者实现

### 同步发送

```java
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Kafka 生产者 - 同步发送示例
 *
 * 特点:
 * - send() 返回 Future
 * - 调用 get() 阻塞等待结果
 * - 适用于需要保证消息发送成功的场景
 */
public class SyncProducerExample {

    /**
     * 创建生产者配置
     *
     * 必要配置项:
     * - bootstrap.servers: Kafka 集群地址
     * - key.serializer: Key 序列化器
     * - value.serializer: Value 序列化器
     */
    public static Properties createProducerConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        // Kafka 集群地址，至少配置一个 broker，会自动发现其他 broker
        // 格式: host1:port1,host2:port2,...
        props.put("bootstrap.servers", "localhost:9092");

        // Key 序列化器
        props.put("key.serializer", StringSerializer.class.getName());

        // Value 序列化器
        props.put("value.serializer", StringSerializer.class.getName());

        // ============ 可靠性配置 ============
        // 确认机制
        // "0": 不等待确认，速度最快，可能丢失数据
        // "1": 等待 Leader 确认
        // "all": 等待所有 ISR 副本确认
        props.put("acks", "all");

        // 发送失败重试次数
        props.put("retries", 3);

        // 重试间隔 (毫秒)
        props.put("retry.backoff.ms", 100);

        // ============ 性能配置 ============
        // 批量发送批次大小 (字节)
        props.put("batch.size", 16384);

        // 发送前等待时间 (毫秒)
        props.put("linger.ms", 1);

        // 生产者缓冲区大小 (字节)
        props.put("buffer.memory", 67108864L);  // 64MB

        return props;
    }

    /**
     * 同步发送消息
     *
     * 执行流程:
     * 1. 创建 ProducerRecord
     * 2. 调用 send() 发送
     * 3. 调用 get() 阻塞等待
     * 4. 处理发送结果
     *
     * @param producer KafkaProducer 实例
     * @param topic    目标 Topic
     * @param key      消息 Key (可为 null)
     * @param value    消息内容
     */
    public static void sendSync(KafkaProducer<String, String> producer,
                                String topic,
                                String key,
                                String value) {
        try {
            // 1. 创建消息记录
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,        // 目标 Topic
                key,          // Key (用于分区选择)
                value         // Value (消息内容)
            );

            // 可选: 设置消息头 (Headers)
            // 用于传递追踪ID、消息类型等元数据
            record.headers().add(new Header("traceId", "trace-001".getBytes()));
            record.headers().add(new Header("source", "order-service".getBytes()));
            record.headers().add(
                new Header("timestamp",
                    String.valueOf(System.currentTimeMillis()).getBytes())
            );

            // 2. 发送消息
            Future<RecordMetadata> future = producer.send(record);

            // 3. 同步等待发送结果
            // 这会阻塞当前线程，直到收到确认或超时
            RecordMetadata metadata = future.get();

            // 4. 处理成功结果
            System.out.println("消息发送成功!");
            System.out.println("  Topic: " + metadata.topic());
            System.out.println("  Partition: " + metadata.partition());
            System.out.println("  Offset: " + metadata.offset());
            System.out.println("  Timestamp: " + metadata.timestamp());
            System.out.println("  CRC32: " + metadata.checksum());

        } catch (InterruptedException e) {
            // 线程被中断 - 恢复中断状态并处理
            Thread.currentThread().interrupt();
            System.err.println("发送被中断: " + e.getMessage());
        } catch (ExecutionException e) {
            // 发送执行失败 - 获取根本原因
            System.err.println("发送失败: " + e.getCause().getMessage());
            e.getCause().printStackTrace();
        }
    }

    /**
     * 批量同步发送
     *
     * 适用场景: 一次发送多条消息
     */
    public static void sendBatchSync(KafkaProducer<String, String> producer,
                                     String topic,
                                     java.util.List<String> values) {
        java.util.List<Future<RecordMetadata>> futures = new java.util.ArrayList<>();

        for (String value : values) {
            try {
                ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, null, value);
                futures.add(producer.send(record));
            } catch (Exception e) {
                System.err.println("发送失败: " + e.getMessage());
            }
        }

        // 等待所有发送完成
        for (int i = 0; i < futures.size(); i++) {
            try {
                RecordMetadata metadata = futures.get(i).get();
                System.out.printf("消息 %d -> Partition: %d, Offset: %d%n",
                    i, metadata.partition(), metadata.offset());
            } catch (Exception e) {
                System.err.printf("消息 %d 发送失败: %s%n", i, e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        Properties props = createProducerConfig();

        // 创建生产者实例
        // KafkaProducer 是线程安全的，可在多线程中共享
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // 发送单条消息
            sendSync(producer, "orders", "order-001",
                "{\"orderId\": 1, \"amount\": 100.0}");

            // 批量发送
            java.util.List<String> orders = java.util.Arrays.asList(
                "{\"orderId\": 2, \"amount\": 200.0}",
                "{\"orderId\": 3, \"amount\": 300.0}",
                "{\"orderId\": 4, \"amount\": 400.0}"
            );
            sendBatchSync(producer, "orders", orders);

        } finally {
            // 关闭生产者
            // flush 发送缓冲区的消息，然后关闭连接
            producer.flush();
            producer.close();
        }
    }
}
```

### 异步发送

```java
import org.apache.kafka.clients.producer.*;

/**
 * Kafka 生产者 - 异步发送示例
 *
 * 特点:
 * - 通过回调函数处理发送结果
 * - 不阻塞当前线程
 * - 适用于高吞吐量场景
 *
 * 注意: 异步发送需要在合适时机调用 flush() 确保消息发送
 */
public class AsyncProducerExample {

    /**
     * 异步发送消息
     *
     * @param producer KafkaProducer 实例
     * @param topic    目标 Topic
     * @param key      消息 Key
     * @param value    消息内容
     */
    public static void sendAsync(KafkaProducer<String, String> producer,
                                 String topic,
                                 String key,
                                 String value) {
        // 创建消息记录
        ProducerRecord<String, String> record = new ProducerRecord<>(
            topic, key, value
        );

        // 设置消息头
        record.headers().add(
            new Header("traceId",
                java.util.UUID.randomUUID().toString().getBytes())
        );

        // 发送消息，指定回调函数
        producer.send(record, new Callback() {
            /**
             * 回调函数 - 消息发送完成后被调用
             *
             * @param metadata 发送结果元数据 (成功时不为 null)
             * @param exception 异常 (成功时为 null)
             */
            @Override
            public void onCompletion(RecordMetadata metadata,
                                     Exception exception) {
                if (exception != null) {
                    // ============ 发送失败处理 ============
                    // 记录错误日志
                    System.err.println("消息发送失败!");
                    System.err.println("  Topic: " + record.topic());
                    System.err.println("  Error: " + exception.getMessage());

                    // 错误分类处理
                    if (exception instanceof TimeoutException) {
                        // 超时 - 可重试
                        handleTimeout(record);
                    } else if (exception instanceof SerializationException) {
                        // 序列化错误 - 需要修复代码
                        handleSerializationError(record, exception);
                    } else {
                        // 其他错误
                        handleOtherError(record, exception);
                    }

                } else {
                    // ============ 发送成功处理 ============
                    System.out.println("消息发送成功!");
                    System.out.println("  Topic: " + metadata.topic());
                    System.out.println("  Partition: " + metadata.partition());
                    System.out.println("  Offset: " + metadata.offset());
                    System.out.println("  Size: " + metadata.serializedValueSize() + " bytes");

                    // 可选: 发送指标到监控系统
                    // metrics.increment("messages_sent");
                    // metrics.recordLatency("send_latency", System.nanoTime() - startTime);
                }
            }
        });
    }

    /**
     * 批量异步发送
     *
     * @param producer KafkaProducer 实例
     * @param topic    目标 Topic
     * @param records  消息列表
     */
    public static void sendBatchAsync(KafkaProducer<String, String> producer,
                                      String topic,
                                      java.util.List<ProducerRecord<String, String>> records) {
        // 统计发送结果
        final int[] successCount = {0};
        final int[] failCount = {0};
        final Object counterLock = new Object();

        for (ProducerRecord<String, String> record : records) {
            producer.send(record, (metadata, exception) -> {
                synchronized (counterLock) {
                    if (exception != null) {
                        failCount[0]++;
                        System.err.printf("Failed: %s%n", record.value());
                    } else {
                        successCount[0]++;
                    }
                }
            });
        }

        // flush 确保所有消息发送
        producer.flush();

        System.out.printf("批量发送完成: 成功 %d, 失败 %d%n",
            successCount[0], failCount[0]);
    }

    /**
     * 带重试的异步发送
     */
    public static void sendWithRetry(KafkaProducer<String, String> producer,
                                     String topic,
                                     String key,
                                     String value,
                                     int maxRetries) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                if (maxRetries > 0) {
                    // 重试发送
                    System.out.println("发送失败，尝试重试...");
                    sendWithRetry(producer, topic, key, value, maxRetries - 1);
                } else {
                    System.err.println("重试次数耗尽，发送失败: " + exception.getMessage());
                    // 写入死信队列
                    sendToDeadLetterQueue(topic, key, value, exception);
                }
            } else {
                System.out.println("发送成功: " + metadata.offset());
            }
        });
    }

    // ============ 错误处理方法 ============

    private static void handleTimeout(ProducerRecord<String, String> record) {
        // 超时处理: 可能需要重试或告警
        System.err.println("消息超时: " + record.value());
    }

    private static void handleSerializationError(ProducerRecord<String, String> record,
                                                 Exception exception) {
        // 序列化错误: 代码 bug，需要修复
        System.err.println("序列化错误: " + exception.getMessage());
    }

    private static void handleOtherError(ProducerRecord<String, String> record,
                                         Exception exception) {
        // 其他错误: 记录详细日志
        System.err.println("未知错误: " + exception.getMessage());
        exception.printStackTrace();
    }

    private static void sendToDeadLetterQueue(String topic, String key,
                                               String value, Exception exception) {
        // 发送到死信队列
        System.err.println("发送到死信队列: " + topic + "-dlq");
    }
}
```

---

## 生产者配置详解

### 配置分类

```
┌─────────────────────────────────────────────────────────────────┐
│                  Kafka Producer 配置分类                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   基础配置       │  │   可靠性配置     │  │   性能配置       │  │
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤  │
│  │ bootstrap.servers│  │    acks         │  │  batch.size     │  │
│  │ key.serializer  │  │    retries      │  │  linger.ms      │  │
│  │ value.serializer│  │  retry.backoff.ms│  │  buffer.memory  │  │
│  │                 │  │  enable.idempotence│ │  max.block.ms  │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   压缩配置       │  │   超时配置       │  │   拦截器配置     │  │
│  ├─────────────────┤  ├─────────────────┤  ├─────────────────┤  │
│  │ compression.type│  │ request.timeout │  │ interceptor.clas│  │
│  │ compression.level│  │ delivery.timeout│  │                 │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 完整配置示例

```java
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * Kafka 生产者完整配置详解
 */
public class ProducerConfigExample {

    /**
     * 高可靠性配置 (适用于生产环境)
     *
     * 特点:
     * - acks=all 确保消息不丢失
     * - 启用幂等性防止重复
     * - 重试机制保证发送成功
     */
    public static Properties reliableConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        // Kafka 集群地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "kafka1:9092,kafka2:9092,kafka3:9092");

        // Key 序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        // Value 序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        // ============ 可靠性配置 ============
        // 确认机制 - all 表示等待所有 ISR 副本确认
        // 这是最可靠的配置，但延迟最高
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 重试间隔 (毫秒)
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // 启用幂等性 (Kafka 2.0+)
        // 防止因重试导致的消息重复
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // ============ 批量发送配置 ============
        // 批量大小 (字节) - 默认 16KB
        // 当批次中消息总大小达到此值时发送
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // 发送前等待时间 (毫秒) - 默认 0
        // 配合 batch.size 使用，等待更多消息加入批次
        // 设置为 5-20ms 可提高吞吐量
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // 缓冲区大小 (字节) - 默认 32MB
        // 用于缓冲待发送的消息
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);  // 64MB

        // ============ 压缩配置 ============
        // 压缩类型: none, gzip, snappy, lz4, zstd
        // lz4: 推荐，压缩比和速度平衡较好
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // 压缩级别: -1 (默认) 或 1-9 ( gzip ) / 0-9 (其他)
        // 更高的压缩率但更慢
        props.put(ProducerConfig.COMPRESSION_LEVEL_CONFIG, -1);

        // ============ 超时配置 ============
        // 请求超时时间 (毫秒) - 默认 30000 (30秒)
        // 等待 Broker 响应的最大时间
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        // 交付超时时间 (毫秒) - 默认 120000 (2分钟)
        // 消息从发送成功到确认的最大总时间
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        // 最大阻塞时间 (毫秒) - 默认 60000 (1分钟)
        // 当 buffer.memory 满时，send() 阻塞的最长时间
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

        // ============ 请求大小配置 ============
        // 单个请求最大大小 (字节) - 默认 1MB
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1048576);

        // ============ 连接配置 ============
        // 连接超时时间 (毫秒) - 默认 10000
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 300000);

        // 重试期间的重新发现时间 (毫秒) - 默认 10000
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 5000);

        return props;
    }

    /**
     * 高吞吐量配置 (适用于日志收集等场景)
     *
     * 特点:
     * - 更大的批量和更长的等待时间
     * - 启用压缩
     * - 降低可靠性要求以换取性能
     */
    public static Properties highThroughputConfig() {
        Properties props = new Properties();

        // 基础配置
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        // 可靠性 - 使用 acks=1 平衡可靠性和性能
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        // 批量发送 - 大批量、长等待
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);      // 64KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);          // 20ms

        // 缓冲区
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 134217728L);  // 128MB

        // 压缩 - lz4 提供最佳速度/压缩比
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return props;
    }

    /**
     * 低延迟配置 (适用于实时交易场景)
     *
     * 特点:
     * - 禁用批量等待
     * - 禁用压缩
     * - 小批次
     */
    public static Properties lowLatencyConfig() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        // 可靠性
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // 禁用批量等待
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        // 禁用压缩
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        // 短超时
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        return props;
    }
}
```

---

## 分区策略

### 默认分区器

```java
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka 分区策略详解
 *
 * 分区器的作用:
 * 1. 决定消息应该发送到哪个分区
 * 2. 保证相同 Key 的消息发送到同一分区 (保证顺序性)
 * 3. 均衡分布在所有分区上
 *
 * 默认分区策略 (DefaultPartitioner):
 * - 如果 key 不为 null: hash(key) % numPartitions
 * - 如果 key 为 null: 粘性分区策略 (Sticky Partitioning)
 */
public class PartitionerExample {

    /**
     * 场景1: 指定 Key - 相同 Key 发送到相同分区
     */
    public static void withKeyExample() {
        String topic = "orders";
        String userId = "user-12345";

        // 相同用户的消息，Key 相同，发送到同一分区
        ProducerRecord<String, String> order1 =
            new ProducerRecord<>(topic, userId, "order-001");
        ProducerRecord<String, String> order2 =
            new ProducerRecord<>(topic, userId, "order-002");
        ProducerRecord<String, String> order3 =
            new ProducerRecord<>(topic, userId, "order-003");

        // 这三条消息会被发送到同一分区 (例如分区 5)
        // 保证同一用户的订单按发送顺序处理
    }

    /**
     * 场景2: 不指定 Key - 粘性分区策略
     */
    public static void withoutKeyExample() {
        String topic = "orders";

        // 没有 Key，使用粘性分区
        ProducerRecord<String, String> record1 =
            new ProducerRecord<>(topic, null, "message-001");
        ProducerRecord<String, String> record2 =
            new ProducerRecord<>(topic, null, "message-002");
        ProducerRecord<String, String> record3 =
            new ProducerRecord<>(topic, null, "message-003");

        // 前几条消息可能发送到同一分区 (等待 batch.size 或 linger.ms)
        // 然后切换到下一个分区
        // 提高批量发送效率
    }

    /**
     * 粘性分区器工作原理
     */
    public static class StickyPartitioner原理 {
        private final ConcurrentMap<String, AtomicInteger> stickyCache =
            new ConcurrentHashMap<>();

        /**
         * 粘性分区逻辑:
         *
         * 1. 第一次发送: 随机选择一个分区
         * 2. 后续发送: 继续使用同一分区，直到:
         *    - 批次已满 (达到 batch.size)
         *    - 达到 linger.ms 时间
         * 3. 批次发送后: 切换到下一个分区
         *
         * 优势:
         * - 减少请求数量，提高吞吐量
         * - 保持负载均衡
         */
        public int selectPartition(String topic, String key, byte[] keyBytes,
                                   Object value, Cluster cluster) {
            List<org.apache.kafka.common.PartitionInfo> partitions =
                cluster.availablePartitionsForTopic(topic);
            int numPartitions = partitions.size();

            if (keyBytes == null) {
                // 无 Key - 使用粘性分区
                String stickyKey = topic;
                AtomicInteger partition = stickyCache.get(stickyKey);

                if (partition == null) {
                    // 初始化为随机分区
                    partition = new AtomicInteger(
                        java.util.concurrent.ThreadLocalRandom.current()
                            .nextInt(numPartitions)
                    );
                    stickyCache.put(stickyKey, partition);
                }

                // 返回当前粘性分区
                return partition.get();
            } else {
                // 有 Key - 使用哈希
                return java.util.Objects.hash(keyBytes) % numPartitions;
            }
        }
    }
}
```

### 自定义分区器

```java
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义分区器示例
 *
 * 使用场景:
 * - 业务需要特定的分区策略
 * - 需要灰度发布 (某些用户走特定分区)
 * - 需要按区域/业务线分区
 */
public class CustomPartitionerExample {

    /**
     * 订单分区器
     *
     * 根据订单类型选择分区:
     * - 紧急订单 (priority) 发送到分区 0
     * - 普通订单 (normal) 发送到其他分区
     * - 其他按 Key 哈希分配
     */
    public static class OrderPartitioner implements Partitioner {

        /**
         * 分区计数缓存 (用于轮询)
         */
        private final ConcurrentMap<String, AtomicInteger> counterMap =
            new ConcurrentHashMap<>();

        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                             Object value, Cluster cluster) {
            // 获取可用分区
            List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
            int numPartitions = partitions.size();

            // ============ 策略1: 按订单类型分区 ============
            if (key instanceof String && ((String) key).startsWith("PRIORITY-")) {
                // 紧急订单 - 发送到分区 0
                // 特点: 优先处理，快速响应
                return 0;
            }

            // ============ 策略2: 按区域分区 ============
            if (key instanceof String && ((String) key).startsWith("CN-")) {
                return 0;  // 中国用户
            } else if (((String) key).startsWith("US-")) {
                return 1;  // 美国用户
            } else if (((String) key).startsWith("EU-")) {
                return 2;  // 欧洲用户
            }

            // ============ 策略3: 按业务线分区 ============
            if (key instanceof String) {
                String keyStr = (String) key;
                if (keyStr.startsWith("ORDER-")) {
                    return 3;  // 订单业务
                } else if (keyStr.startsWith("PAYMENT-")) {
                    return 4;  // 支付业务
                } else if (keyStr.startsWith("REFUND-")) {
                    return 5;  // 退款业务
                }
            }

            // ============ 策略4: 轮询分配 ============
            // 使用 Key 哈希 (保证相同 Key 同一分区)
            if (key != null) {
                return Math.abs(key.hashCode()) % numPartitions;
            }

            // 无 Key - 轮询分配
            String topicKey = topic + "-round-robin";
            int partition = counterMap.computeIfAbsent(topicKey,
                k -> new AtomicInteger(0)).getAndIncrement() % numPartitions;

            return partition;
        }

        @Override
        public void close() {
            // 关闭分区器，清理资源
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // 配置分区器参数
        }
    }

    /**
     * 会话分区器
     *
     * 根据会话ID分区，保证同一会话的消息有序
     */
    public static class SessionPartitioner implements Partitioner {

        @Override
        public int partition(String topic, Object key, byte[] keyBytes,
                             Object value, Cluster cluster) {
            List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
            int numPartitions = partitions.size();

            // 从 Key 中提取会话ID
            String sessionId = extractSessionId(key);

            if (sessionId != null) {
                // 使用会话ID哈希，保证同一会话同一分区
                return Math.abs(sessionId.hashCode()) % numPartitions;
            }

            // 无会话ID - 使用随机分区
            return java.util.concurrent.ThreadLocalRandom.current()
                .nextInt(numPartitions);
        }

        private String extractSessionId(Object key) {
            if (key instanceof String) {
                String keyStr = (String) key;
                // 假设格式: sessionId:messageId
                int colonIndex = keyStr.indexOf(':');
                if (colonIndex > 0) {
                    return keyStr.substring(0, colonIndex);
                }
                return keyStr;
            }
            return null;
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}
    }

    /**
     * 使用自定义分区器
     */
    public static void useCustomPartitioner() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 配置自定义分区器
        props.put("partitioner.class", "com.example.OrderPartitioner");

        // 可选: 分区器额外配置
        // props.put("partitioner.adaptive.partitioning.enable", true);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送消息
        producer.send(new ProducerRecord<>("orders", "PRIORITY-order-001", "紧急订单"),
            (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("紧急订单发送到分区: " + metadata.partition());
                }
            });
    }
}
```

---

## 序列化器

### 内置序列化器

```java
import org.apache.kafka.common.serialization.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Kafka 序列化器详解
 *
 * 序列化器作用:
 * - 将 Java 对象转换为字节数组
 * - 发送前对消息进行编码
 *
 * 内置序列化器:
 * | 序列化器                  | 说明                      |
 * |-------------------------|--------------------------|
 * | StringSerializer        | UTF-8 字符串              |
 * | IntegerSerializer       | 整数                      |
 * | LongSerializer          | 长整数                    |
 * | DoubleSerializer        | 双精度浮点数               |
 * | ByteArraySerializer     | 字节数组                  |
 * | ByteBufferSerializer    | ByteBuffer               |
 * | BytesSerializer         | Kafka Bytes              |
 */
public class SerializerExample {

    /**
     * StringSerializer 使用
     */
    public static void stringSerializer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送 JSON 字符串
        String json = "{\"orderId\": 1, \"amount\": 100.0}";
        producer.send(new ProducerRecord<>("orders", "order-1", json));

        // 发送普通文本
        producer.send(new ProducerRecord<>("logs", null, "INFO: Application started"));
    }

    /**
     * 整数序列化器使用
     */
    public static void integerSerializer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", IntegerSerializer.class.getName());
        props.put("value.serializer", LongSerializer.class.getName());

        KafkaProducer<Integer, Long> producer = new KafkaProducer<>(props);

        // Key 是整数 (如用户ID), Value 是时间戳
        producer.send(new ProducerRecord<>("events", 1001, System.currentTimeMillis()));
    }
}

/**
 * 自定义序列化器
 *
 * 适用场景:
 * - 复杂对象序列化
 * - 第三方库对象
 * - 特定格式需求
 */
class CustomSerializerExample {

    /**
     * 用户订单对象
     */
    public static class Order {
        private long orderId;
        private String userId;
        private double amount;
        private long timestamp;

        // 构造函数、getter、setter
        public Order(long orderId, String userId, double amount) {
            this.orderId = orderId;
            this.userId = userId;
            this.amount = amount;
            this.timestamp = System.currentTimeMillis();
        }

        // 转 JSON 字符串
        public String toJson() {
            return String.format(
                "{\"orderId\":%d,\"userId\":\"%s\",\"amount\":%.2f,\"timestamp\":%d}",
                orderId, userId, amount, timestamp
            );
        }
    }

    /**
     * JSON 序列化器
     *
     * 将对象序列化为 JSON 字符串字节数组
     */
    public static class JsonSerializer<T> implements Serializer<T> {

        @Override
        public byte[] serialize(String topic, T data) {
            if (data == null) {
                return null;
            }

            if (data instanceof String) {
                return ((String) data).getBytes(StandardCharsets.UTF_8);
            }

            if (data instanceof Order) {
                return ((Order) data).toJson()
                    .getBytes(StandardCharsets.UTF_8);
            }

            // 使用 Jackson 或 Gson 序列化
            try {
                // ObjectMapper mapper = new ObjectMapper();
                // return mapper.writeValueAsBytes(data);
                return data.toString().getBytes(StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new SerializationException("序列化失败", e);
            }
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {}
    }

    /**
     * Avro 序列化器
     *
     * 使用 Schema Registry 管理 Schema
     */
    public static class AvroSerializerExample {

        /**
         * Avro 序列化配置
         */
        public static Properties avroConfig() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092");
            props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
            props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

            // Schema Registry 地址
            props.put("schema.registry.url", "http://localhost:8081");

            // 可靠性配置
            props.put("auto.register.schemas", true);
            props.put("max.schema.incomparability", "0");

            return props;
        }
    }

    /**
     * 使用自定义序列化器
     */
    public static void useCustomSerializer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", JsonSerializer.class.getName());

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);

        Order order = new Order(1001, "user-123", 99.99);
        producer.send(new ProducerRecord<>("orders", "order-1001", order));
    }
}
```

---

## 压缩机制

### 压缩配置

```java
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * Kafka 消息压缩详解
 *
 * 压缩优势:
 * - 减少网络带宽使用
 * - 降低磁盘存储空间
 * - 提高吞吐量
 *
 * 压缩类型对比:
 * | 类型     | 压缩比 | 压缩速度 | CPU 使用 | 适用场景              |
 * |----------|--------|----------|----------|---------------------|
 * | none     | 1x     | 最快     | 最低     | 短消息、低延迟        |
 * | gzip     | 高     | 中       | 中       | 通用场景              |
 * | snappy   | 中     | 快       | 低       | 日志、JSON 数据       |
 * | lz4      | 高     | 最快     | 低       | 大批量数据 (推荐)     |
 * | zstd     | 最高   | 快       | 中       | 高压缩比需求          |
 *
 * 注意: 压缩是批量进行的，消息太少时压缩效果不明显
 */
public class CompressionExample {

    /**
     * 压缩配置示例
     */
    public static void compressionConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // ============ 压缩类型配置 ============
        // none: 不压缩 (默认)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        // gzip: 平衡压缩比和速度
        // 适用于: 中等大小消息，需要较高压缩比
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        // snappy: 快速压缩，低 CPU 使用
        // 适用于: 日志数据、JSON，数据量大
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // lz4: 快速压缩，高压缩比 (推荐)
        // 适用于: 大批量数据，高吞吐量场景
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // zstd: 最高压缩比
        // 适用于: 存储空间有限的场景
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        // ============ 压缩级别配置 ============
        // compression.level: 压缩级别
        // gzip: -1 (默认,平衡) 或 1-9 (1最快,9最高压缩比)
        props.put(ProducerConfig.COMPRESSION_LEVEL_CONFIG, 6);
    }

    /**
     * 批量发送展示压缩效果
     */
    public static void batchCompressionExample() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // 启用 LZ4 压缩
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // 配置批量发送
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 131072);  // 128KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);       // 20ms 等待

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 批量发送消息
        for (int i = 0; i < 1000; i++) {
            String json = String.format(
                "{\"id\":%d,\"data\":\"%s\",\"timestamp\":%d}",
                i, "x".repeat(100), System.currentTimeMillis()
            );

            producer.send(new ProducerRecord<>("logs", String.valueOf(i), json));
        }

        producer.flush();
        producer.close();

        // 压缩效果:
        // 原始数据: ~130KB
        // 压缩后: ~30KB (约 23%)
        // 节省: ~77% 带宽
    }
}
```

---

## 拦截器

### 生产者拦截器

```java
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka 生产者拦截器
 *
 * 作用:
 * - 在消息发送前修改消息
 * - 在发送完成后处理结果
 * - 收集发送指标
 *
 * 使用场景:
 * - 消息追踪
 - 审计日志
 * - 消息修改
 * - 指标收集
 */
public class ProducerInterceptorExample {

    /**
     * 追踪拦截器
     *
     功能:
     * - 添加追踪ID
     * - 记录发送时间
     * - 收集发送结果
     */
    public static class TracingInterceptor implements ProducerInterceptor<String, String> {

        private final AtomicLong sendCounter = new AtomicLong(0);
        private final AtomicLong successCounter = new AtomicLong(0);
        private final AtomicLong failureCounter = new AtomicLong(0);

        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            // 发送前拦截 - 可以修改消息

            // 添加发送时间戳
            long sendTime = System.currentTimeMillis();
            record.headers().add(
                new Header("sendTime", String.valueOf(sendTime).getBytes())
            );

            // 添加追踪ID
            String traceId = java.util.UUID.randomUUID().toString();
            record.headers().add(
                new Header("traceId", traceId.getBytes())
            );

            // 记录发送
            sendCounter.incrementAndGet();

            System.out.printf("[TRACE] 发送消息: topic=%s, traceId=%s%n",
                record.topic(), traceId);

            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            // 发送完成后拦截

            if (exception == null) {
                successCounter.incrementAndGet();
                System.out.printf("[TRACE] 发送成功: topic=%s, partition=%d, offset=%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                failureCounter.incrementAndGet();
                System.err.printf("[TRACE] 发送失败: %s%n", exception.getMessage());
            }
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}

        // 统计信息
        public long getSendCount() { return sendCounter.get(); }
        public long getSuccessCount() { return successCounter.get(); }
        public long getFailureCount() { return failureCounter.get(); }
    }

    /**
     * 消息修改拦截器
     *
     * 功能:
     * - 添加元数据
     * - 消息内容转换
     */
    public static class MessageModifierInterceptor implements ProducerInterceptor<String, String> {

        private String clientId;

        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            // 添加客户端ID
            record.headers().add(
                new Header("clientId", clientId.getBytes())
            );

            // 添加消息版本
            record.headers().add(
                new Header("messageVersion", "1.0".getBytes())
            );

            // 可选: 修改消息内容
            // String newValue = transformValue(record.value());
            // return new ProducerRecord<>(record.topic(), record.key(), newValue);

            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {
            // 从配置中获取 clientId
            clientId = (String) configs.get("client.id");
        }
    }

    /**
     * 指标收集拦截器
     */
    public static class MetricsInterceptor implements ProducerInterceptor<String, String> {

        private final AtomicLong totalBytesSent = new AtomicLong(0);
        private final AtomicLong totalMessagesSent = new AtomicLong(0);

        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                totalBytesSent.addAndGet(metadata.serializedValueSize());
                totalMessagesSent.incrementAndGet();
            }
        }

        @Override
        public void close() {}

        @Override
        public void configure(Map<String, ?> configs) {}

        public double getAverageMessageSize() {
            long count = totalMessagesSent.get();
            if (count == 0) return 0;
            return (double) totalBytesSent.get() / count;
        }
    }

    /**
     * 使用拦截器
     */
    public static void useInterceptors() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // 配置拦截器链 (多个用逗号分隔)
        props.put("interceptor.classes",
            "com.example.TracingInterceptor," +
            "com.example.MetricsInterceptor");

        // 拦截器配置
        props.put("client.id", "order-service");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        producer.send(new ProducerRecord<>("orders", "order-001", "test"));

        producer.close();
    }
}
```

---

## 最佳实践

### 生产环境配置

```java
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Kafka Producer 最佳实践
 */
public class ProducerBestPractices {

    /**
     * 生产环境推荐配置
     */
    public static Properties productionConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "kafka1:9092,kafka2:9092,kafka3:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());

        // ============ 可靠性 (生产环境必须) ============
        // 启用幂等性 (Kafka 2.0+)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 确认机制
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 重试配置
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // ============ 性能优化 ============
        // 批量大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);  // 32KB

        // 等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);      // 10ms

        // 缓冲区
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864L);  // 64MB

        // 压缩
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // ============ 超时配置 ============
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000);

        return props;
    }

    /**
     * 错误处理最佳实践
     */
    public static void errorHandlingBestPractice() {
        Properties props = productionConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record =
            new ProducerRecord<>("orders", "key", "value");

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                // 1. 分类处理错误
                if (exception instanceof TimeoutException) {
                    // 超时 - 可重试
                    handleTimeout(record);
                } else if (exception instanceof SerializationException) {
                    // 序列化错误 - 不能重试，需要修复代码
                    handleSerializationError(exception);
                } else if (exception instanceof KafkaException) {
                    // Kafka 相关错误 - 可能需要重试
                    handleKafkaError(exception);
                }

                // 2. 记录错误日志
                logError(record, exception);

                // 3. 发送到死信队列
                sendToDeadLetterQueue(record, exception);
            }
        });
    }

    private static void handleTimeout(ProducerRecord<String, String> record) {
        System.err.println("消息超时: " + record.value());
        // 可选: 重试
    }

    private static void handleSerializationError(Exception exception) {
        System.err.println("序列化错误: " + exception.getMessage());
    }

    private static void handleKafkaError(Exception exception) {
        System.err.println("Kafka 错误: " + exception.getMessage());
    }

    private static void logError(ProducerRecord<String, String> record,
                                  Exception exception) {
        // 记录到日志系统
    }

    private static void sendToDeadLetterQueue(ProducerRecord<String, String> record,
                                               Exception exception) {
        // 发送到死信队列
    }

    /**
     * 资源管理最佳实践
     */
    public static void resourceManagement() {
        KafkaProducer<String, String> producer = null;
        try {
            Properties props = productionConfig();
            producer = new KafkaProducer<>(props);

            // 发送消息
            producer.send(new ProducerRecord<>("topic", "key", "value"));

            // 确保消息发送
            producer.flush();

        } finally {
            // 1. 先 flush
            if (producer != null) {
                producer.flush();
            }

            // 2. 再 close
            if (producer != null) {
                // close 会等待发送完成，超时时间可配置
                producer.close(Duration.ofSeconds(30));
            }
        }
    }

    /**
     * 监控指标
     */
    public static void monitoringMetrics() {
        Properties props = productionConfig();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 获取指标
        org.apache.kafka.common.metrics.KafkaMetricsContext metricsContext =
            (org.apache.kafka.common.metrics.KafkaMetricsContext)
                producer.metrics();

        // 关键指标
        // producer-metrics: {producer-metrics}
        // record-send-total: 发送消息总数
        // record-retry-total: 重试次数
        // record-error-total: 发送错误次数
        // batch-size-avg: 平均批次大小
        // record-latency-avg: 平均延迟

        // 示例: 获取发送速率
        // double sendRate = producer.metrics().get("record-send-rate").value();

        producer.close();
    }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Kafka 架构详解](01-architecture.md) | Kafka 核心架构和概念 |
| [Kafka Consumer 指南](03-consumer.md) | 消费者 API 和配置详解 |
| [Kafka Streams 指南](04-streams.md) | 流处理 API 详解 |
| [Kafka 运维指南](05-operations.md) | 集群部署和运维 |
| [Kafka 故障排查](06-troubleshooting.md) | 常见问题和解决方案 |
