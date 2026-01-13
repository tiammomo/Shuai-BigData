# Kafka Streams 详解

## 目录

- [Kafka Streams 核心概念](#kafka-streams-核心概念)
- [ Streams API 基础](#streams-api-基础)
- [核心组件](#核心组件)
- [状态管理](#状态管理)
- [窗口操作](#窗口操作)
- [连接器](#连接器)
- [处理器拓扑](#处理器拓扑)
- [最佳实践](#最佳实践)

---

## Kafka Streams 核心概念

### 什么是 Kafka Streams?

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka Streams 简介                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Kafka Streams 是一个 Java 客户端库，用于构建:                    │
│  - 实时流处理应用                                                │
│  - 微服务                                                       │
│  - 事件驱动系统                                                  │
│                                                                 │
│  核心特性:                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ✓ 轻量级 - 无需集群，嵌入应用                           │   │
│  │  ✓ 可扩展 - 自动并行处理                                 │   │
│  │  ✓ 容错 - 自动恢复失败任务                               │   │
│  │  ✓ Exactly-Once - 精确一次语义                           │   │
│  │  ✓ 弹性 - 支持扩缩容                                     │   │
│  │  ✓ 高效 - 低延迟流处理                                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  vs 其他流处理框架:                                              │
│                                                                 │
│  | 特性        | Kafka Streams | Flink  | Spark Streaming |    │
│  |------------|---------------|--------|-----------------|    │
│  | 部署复杂度  | 低            | 中     | 高              |    │
│  | 延迟        | 低 (~ms)      | 低     | 高 (~s)         |    │
│  | 状态管理    | 内置          | 内置   | 需额外组件      |    │
│  | 容错机制    | 自动          | 自动   | 自动            |    │
│  | 编程模型    | DSL/API       | DSL/API| RDD            |    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 处理架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Kafka Streams 处理架构                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  输入 Topic                    Streams 应用                     输出 Topic│
│  ┌─────────────┐        ┌─────────────────────────┐        ┌─────────┐ │
│  │ orders      │  --->  │  ┌───────────────────┐  │  --->  │ orders  │ │
│  │ (原始数据)   │        │  │  Stream Tasks     │  │        │(聚合结果)│ │
│  └─────────────┘        │  │  (并行处理)        │  │        └─────────┘ │
│                         │  └───────────────────┘  │                       │
│  ┌─────────────┐        │  ┌───────────────────┐  │        ┌─────────┐ │
│  │ payments    │  --->  │  │  State Stores     │  │  --->  │ alerts  │ │
│  │ (支付数据)   │        │  │  (状态存储)        │  │        │(告警)   │ │
│  └─────────────┘        │  └───────────────────┘  │        └─────────┘ │
│                         └─────────────────────────┘                       │
│                                  │                                        │
│                                  ▼                                        │
│                         ┌───────────────────┐                            │
│                         │  RocksDB / 内存   │                            │
│                         │  (本地状态存储)    │                            │
│                         └───────────────────┘                            │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Kafka Cluster                                │   │
│  │  - 输入/输出 Topic                                               │   │
│  │  - 消费者组协调                                                  │   │
│  │  - 偏移量管理                                                    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Streams API 基础

### Maven 依赖

```xml
<!-- Kafka Streams 依赖 -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
    <version>3.7.0</version>
</dependency>

<!-- 测试依赖 -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>streams-test</artifactId>
    <version>3.7.0</version>
    <scope>test</scope>
</dependency>
```

### 简单示例

```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams 入门示例
 *
 * 功能: 统计单词出现次数
 *
 * 输入: 文本消息流
 * 处理: 拆分单词，统计计数
 * 输出: 单词计数流
 */
public class WordCountExample {

    /**
     * 创建 Streams 配置
     */
    public static Properties createConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        // 应用 ID (必须唯一，用于消费者组)
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");

        // Kafka 集群地址
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // 默认序列化/反序列化器
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName());

        // ============ 处理配置 ============
        // 缓存大小 (字节)
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024);

        // ============ 容错配置 ============
        // 复制因子
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

        // 提交间隔 (毫秒)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        return props;
    }

    /**
     * 构建流处理拓扑
     *
     * 处理流程:
     * 1. 从 Topic 读取数据
     * 2. 拆分单词
     * 3. 转换为 (word, 1) 对
     * 4. 分组
     * 5. 聚合计数
     * 6. 输出到 Topic
     */
    public static void buildWordCountTopology(StreamsBuilder builder) {
        // 1. 从 Topic 读取数据流
        // KStream: 无界流，不可更新
        KStream<String, String> source = builder.stream("wordcount-input");

        // 2. 处理流
        // flatMapValues: 对每个值应用函数，输出多个值
        KStream<String, Long> wordCounts = source
            // 2.1 拆分单词 (按空格分割)
            .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\s+")))

            // 2.2 选择 Key (使用单词作为 Key)
            .selectKey((key, word) -> word)

            // 2.3 分组
            .groupByKey()

            // 2.4 聚合计数
            .count(Materialized.as("word-count-store"))

            // 2.5 转换为 KStream
            .toStream();

        // 3. 输出到 Topic
        wordCounts.to("wordcount-output", Produced.with(
            Serdes.String(),  // Key 序列化器
            Serdes.Long()     // Value 序列化器
        ));
    }

    /**
     * 运行 Streams 应用
     */
    public static void run() throws InterruptedException {
        // 1. 创建配置
        Properties props = createConfig();

        // 2. 构建拓扑
        StreamsBuilder builder = new StreamsBuilder();
        buildWordCountTopology(builder);

        // 3. 创建 Streams 实例
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // 4. 添加关闭钩子
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        // 5. 启动
        System.out.println("启动 WordCount 应用...");
        streams.start();

        // 6. 等待关闭
        latch.await();
    }

    /**
     * 完整示例
     */
    public static class CompleteExample {

        public static void main(String[] args) throws InterruptedException {
            Properties props = createConfig();

            StreamsBuilder builder = new StreamsBuilder();

            // 从 Topic 读取
            KStream<String, String> source = builder.stream("wordcount-input");

            // 处理
            KTable<String, Long> counts = source
                .flatMapValues(value ->
                    Arrays.asList(value.toLowerCase().split("\\s+")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .count(Materialized.as("counts"));

            // 输出
            counts.toStream().to("wordcount-output",
                Produced.with(Serdes.String(), Serdes.Long()));

            // 创建并启动
            KafkaStreams streams = new KafkaStreams(builder.build(), props);

            streams.setStateListener((newState, oldState) -> {
                System.out.printf("状态变更: %s -> %s%n", oldState, newState);
            });

            streams.start();
        }
    }
}
```

### 核心概念

```java
import org.apache.kafka.streams.kstream.*;

/**
 * Kafka Streams 核心概念
 *
 * 1. KStream - 流
 *    - 无界数据序列
 *    - 每条记录独立
 *    - 类似数据库表的所有记录
 *
 * 2. KTable - 表
 *    - 有界数据视图
 *    - Key 唯一
 *    - 类似数据库表的最新快照
 *
 * 3. GlobalKTable - 全局表
 *    - 每个任务持有完整数据
 *    - 用于维度表关联
 */
public class CoreConcepts {

    /**
     * KStream vs KTable 对比
     */
    public static void streamVsTable() {
        StreamsBuilder builder = new StreamsBuilder();

        // ============ KStream 示例 ============
        // 事件流: 每条记录都是独立事件
        KStream<String, String> events = builder.stream("events");
        // 输入: (user1, "login"), (user1, "logout"), (user1, "login")
        // 输出: 3 条独立记录

        // ============ KTable 示例 ============
        // 表视图: 相同 Key 的最后一条记录
        KTable<String, String> table = builder.table("user-states");
        // 输入: (user1, "login"), (user1, "logout"), (user1, "login")
        // 输出: 1 条记录 (user1, "login")

        /**
         * 场景对比:
         *
         * KStream 适用:
         * - 事件日志
         * - 交易记录
         * - 传感器数据
         *
         * KTable 适用:
         * - 用户状态
         * - 配置信息
         * - 最新价格
         */
    }

    /**
     * KStream 和 KTable 互相转换
     */
    public static void conversions() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("input-topic");

        // KStream -> KTable: 聚合
        // 按 Key 分组，取最后值 (即 KTable)
        KTable<String, String> table = source
            .groupByKey()
            .reduce((v1, v2) -> v2);  // 取最新值

        // KTable -> KStream: toStream
        KStream<String, String> stream = table.toStream();

        // KStream -> KTable: groupBy + count + toTable
        KTable<String, Long> countTable = source
            .groupBy((k, v) -> KeyValue.pair(k, v))
            .count()
            .toStream()
            .toTable();
    }

    /**
     * GlobalKTable
     *
     * 特点:
     * - 每个 Streams 任务持有完整数据
     * - 无需分区键关联
     * - 适合维度表
     */
    public static void globalKTableExample() {
        StreamsBuilder builder = new StreamsBuilder();

        // 订单流 (按订单ID分区)
        KStream<String, Order> orders = builder.stream("orders");

        // 用户维度表 (GlobalKTable)
        GlobalKTable<String, User> users = builder.globalTable("users");

        // 关联订单和用户信息
        KStream<String, OrderWithUser> enrichedOrders = orders.join(
            users,
            (orderKey, order) -> order.getUserId(),  // 订单流: 获取用户ID
            (order, user) -> new OrderWithUser(order, user)  // 合并
        );

        enrichedOrders.to("enriched-orders");
    }

    // 数据模型
    public static class Order {
        private String orderId;
        private String userId;
        private double amount;
        // getter/setter
    }

    public static class User {
        private String userId;
        private String name;
        // getter/setter
    }

    public static class OrderWithUser {
        private Order order;
        private User user;
        // getter/setter
    }
}
```

---

## 核心组件

### 转换操作

```java
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KeyValue;
import java.util.Arrays;
import java.util.List;

/**
 * Kafka Streams 转换操作详解
 */
public class TransformationOperations {

    /**
     * 1. 无状态转换 (Stateless Transformations)
     *
     * 不依赖状态，直接转换记录
     */
    public static class StatelessTransformations {

        public static void examples(StreamsBuilder builder) {
            KStream<String, String> source = builder.stream("input");

            // map: 转换 Key 和 Value
            KStream<String, Integer> mapped = source.map(
                (key, value) -> KeyValue.pair(
                    key.toUpperCase(),                    // 转换 Key
                    value.length()                        // 转换 Value
                )
            );

            // mapValues: 只转换 Value
            KStream<String, String> valuesMapped = source.mapValues(
                value -> value.toUpperCase()
            );

            // flatMap: 转换并展开
            KStream<String, String> flatMapped = source.flatMap(
                (key, value) -> {
                    List<KeyValue<String, String>> result = new java.util.ArrayList<>();
                    for (String word : value.split(" ")) {
                        result.add(KeyValue.pair(word, "1"));
                    }
                    return result;
                }
            );

            // flatMapValues: 只转换 Value 并展开
            KStream<String, String> flatValuesMapped = source.flatMapValues(
                value -> Arrays.asList(value.split(","))
            );

            // filter: 过滤
            KStream<String, String> filtered = source.filter(
                (key, value) -> value.length() > 10
            );

            // filterNot: 反向过滤
            KStream<String, String> filteredNot = source.filterNot(
                (key, value) -> value.isEmpty()
            );

            // branch: 分支
            KStream<String, String>[] branches = source.branch(
                (key, value) -> value.startsWith("A"),  // 条件1
                (key, value) -> value.startsWith("B"),  // 条件2
                (key, value) -> true                    // 默认
            );
            KStream<String, String> aStream = branches[0];
            KStream<String, String> bStream = branches[1];
            KStream<String, String> otherStream = branches[2];

            // merge: 合并
            KStream<String, String> merged = aStream.merge(otherStream);
        }
    }

    /**
     * 2. 有状态转换 (Stateful Transformations)
     *
     * 需要维护状态，如聚合、连接等
     */
    public static class StatefulTransformations {

        public static void examples(StreamsBuilder builder) {
            KStream<String, Long> source = builder.stream("input");

            // ============ 聚合 (Aggregation) ============
            // count: 计数
            KTable<String, Long> counts = source
                .groupByKey()
                .count();

            // sum: 求和
            KTable<String, Long> sums = source
                .groupByKey()
                .sum();

            // reduce: 归约
            KTable<String, Long> reduced = source
                .groupByKey()
                .reduce((v1, v2) -> v1 + v2);

            // aggregate: 自定义聚合
            KTable<String, List<String>> aggregated = source
                .groupByKey()
                .aggregate(
                    () -> new java.util.ArrayList<String>(),  // 初始值
                    (key, value, list) -> {
                        list.add(value);
                        return list;
                    },
                    Materialized.as("aggregate-store")
                );

            // ============ 连接 (Join) ============
            KStream<String, String> left = builder.stream("left-topic");
            KStream<String, String> right = builder.stream("right-topic");

            // Join: 基于相同 Key 连接两个流
            KStream<String, String> joined = left.join(
                right,
                (leftValue, rightValue) -> leftValue + "-" + rightValue,  // Value 合并
                JoinWindows.ofTimeDifferenceAndGrace(
                    java.time.Duration.ofMinutes(5),  // 窗口大小
                    java.time.Duration.ofMinutes(1)   // 容忍时间
                )
            );

            // Left Join
            KStream<String, String> leftJoined = left.leftJoin(
                right,
                (leftValue, rightValue) -> leftValue + "-" + rightValue
            );

            // Outer Join
            KStream<String, String> outerJoined = left.outerJoin(
                right,
                (leftValue, rightValue) -> leftValue + "-" + rightValue
            );

            // ============ KTable Join ============
            KTable<String, String> table1 = builder.table("table1");
            KTable<String, String> table2 = builder.table("table2");

            // Table Join
            KTable<String, String> tableJoined = table1.join(
                table2,
                (v1, v2) -> v1 + v2
            );

            // Table Left Join
            KTable<String, String> tableLeftJoined = table1.leftJoin(
                table2,
                (v1, v2) -> v1 + (v2 != null ? v2 : "")
            );
        }
    }

    /**
     * 3. 分组操作
     */
    public static void groupingExamples(StreamsBuilder builder) {
        KStream<String, String> source = builder.stream("input");

        // groupBy: 分组 (可以重新选择 Key)
        // 会隐式进行重新分区
        KGroupedStream<String, String> grouped = source.groupBy(
            (key, value) -> value.substring(0, 1)  // 按首字母分组
        );

        // groupByKey: 分组 (保持 Key)
        // 不会重新分区
        KGroupedStream<String, String> groupedByKey = source.groupByKey();

        // 分组后操作
        grouped.count();
        grouped.reduce((v1, v2) -> v2);
        grouped.aggregate(
            () -> 0L,
            (k, v, agg) -> agg + 1
        );
    }
}
```

---

## 状态管理

### 状态存储

```java
import org.apache.kafka.streams.state.*;

/**
 * Kafka Streams 状态管理详解
 *
 * 状态类型:
 * 1. RocksDB - 持久化状态存储 (默认)
 * 2. InMemory - 内存状态存储
 * 3. Window Store - 窗口状态存储
 * 4. Session Store - 会话状态存储
 */
public class StateManagement {

    /**
     * 1. KeyValue Store
     *
     * 键值对存储，类似于 HashMap
     */
    public static void keyValueStore(StreamsBuilder builder) {
        KStream<String, String> source = builder.stream("input");

        // 创建 KeyValue Store
        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> materialized =
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long());

        // 使用自定义 Store
        source
            .groupByKey()
            .aggregate(
                () -> 0L,  // 初始值
                (key, value, aggregate) -> aggregate + 1,
                materialized
            );

        // 访问自定义 Store
        // 使用 transform 或 transformValues
        source.transform(
            () -> new KeyValueStoreTransformer(),
            "count-store"
        );
    }

    /**
     * 2. Window Store
     *
     * 窗口状态存储，支持时间窗口
     */
    public static void windowStore(StreamsBuilder builder) {
        // 创建窗口
        TimeWindows windows = TimeWindows.ofSizeAndGrace(
            java.time.Duration.ofMinutes(5),  // 窗口大小
            java.time.Duration.ofMinutes(1)   // 容忍时间
        );

        // 使用窗口 Store
        KStream<String, String> source = builder.stream("input");

        source
            .groupByKey()
            .windowedBy(windows)  // 指定窗口
            .count(Materialized.as("windowed-count-store"));

        // 访问窗口 Store
        // 使用 transformValues with Store
        source.transformValues(
            () -> new WindowStoreTransformer(),
            "windowed-count-store"
        );
    }

    /**
     * 3. Session Store
     *
     * 会话状态存储，用于用户会话分析
     */
    public static void sessionStore(StreamsBuilder builder) {
        // 会话窗口
        SessionWindows windows = SessionWindows.with(
            java.time.Duration.ofMinutes(30)  // 非活动间隔
        );

        KStream<String, String> source = builder.stream("input");

        source
            .groupByKey()
            .windowedBy(windows)
            .count(Materialized.as("session-count-store"));
    }

    /**
     * 4. 自定义 Store
     *
     * 实现 StateStore 接口
     */
    public static class CustomStoreExample {

        /**
         * 自定义处理器
         */
        public static class CountProcessor
                implements org.apache.kafka.streams.processor.Processor<String, String> {

            private org.apache.kafka.streams.processor.ProcessorContext context;
            private KeyValueStore<String, Long> countStore;

            @Override
            public void init(org.apache.kafka.streams.processor.ProcessorContext context) {
                this.context = context;
                // 获取 Store
                this.countStore = context.getStateStore("counts");
            }

            @Override
            public void process(String key, String value) {
                // 读取当前计数
                Long count = countStore.get(key);
                if (count == null) {
                    count = 0L;
                }

                // 更新计数
                countStore.put(key, count + 1);

                // 转发结果
                context.forward(key, count.toString());
            }

            @Override
            public void close() {
                // 清理资源
            }
        }

        /**
         * 自定义 Transformer
         */
        public static class CountTransformer
                implements Transformer<String, String, KeyValue<String, String>> {

            private KeyValueStore<String, Long> countStore;

            @Override
            public void init(org.apache.kafka.streams.processor.ProcessorContext context) {
                this.countStore = context.getStateStore("counts");
            }

            @Override
            public KeyValue<String, String> transform(String key, String value) {
                Long count = countStore.get(key);
                count = (count == null) ? 1L : count + 1;
                countStore.put(key, count);
                return KeyValue.pair(key, count.toString());
            }

            @Override
            public void close() {}
        }
    }

    /**
     * 5. 内存 vs RocksDB
     */
    public static void storeTypeComparison() {
        StreamsBuilder builder = new StreamsBuilder();

        // 内存 Store (测试用)
        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> inMemoryStore =
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("in-memory-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
                .withLoggingDisabled();  // 禁用 RocksDB 日志

        // RocksDB Store (生产用，默认)
        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> rocksDBStore =
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("rocksdb-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long())
                .withCachingEnabled();  // 启用缓存

        /**
         * 内存 Store:
         * 优点: 快速，适合小数据量、测试
         * 缺点: 内存有限，重启丢失
         *
         * RocksDB:
         * 优点: 支持大数据量，持久化
         * 缺点: 相对较慢
         */
    }
}
```

---

## 窗口操作

### 窗口类型

```java
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;

/**
 * Kafka Streams 窗口操作详解
 *
 * 窗口类型:
 * 1. Tumbling Window - 滚动窗口
 * 2. Hopping Window - 跳跃窗口
 * 3. Sliding Window - 滑动窗口
 * 4. Session Window - 会话窗口
 */
public class WindowOperations {

    /**
     * 1. Tumbling Window (滚动窗口)
     *
     * 特点:
     * - 不重叠
     * - 固定大小
     * - 按时钟对齐
     *
     * 时间线:
     * [----窗口1----][----窗口2----][----窗口3----]
     * 00:00-00:05   00:05-00:10   00:10-00:15
     */
    public static void tumblingWindow(StreamsBuilder builder) {
        KStream<String, Long> source = builder.stream("input");

        // 创建滚动窗口 (5分钟)
        TimeWindows windows = TimeWindows.ofSizeAndGrace(
            Duration.ofMinutes(5),      // 窗口大小
            Duration.ofMinutes(1)       // 容忍时间 (允许迟到)
        );

        // 使用滚动窗口聚合
        source
            .groupByKey()
            .windowedBy(windows)
            .count()
            .toStream()
            .to("tumbling-window-output");
    }

    /**
     * 2. Hopping Window (跳跃窗口)
     *
     * 特点:
     * - 可重叠
     * - 固定大小
     * - 固定步长
     *
     * 时间线:
     * [窗口1----][  步长  ][窗口2----][  步长  ][窗口3----]
     * 00:00-00:05           00:03-00:08           00:06-00:11
     */
    public static void hoppingWindow(StreamsBuilder builder) {
        KStream<String, Long> source = builder.stream("input");

        // 创建跳跃窗口
        TimeWindows windows = TimeWindows.ofSizeAndGrace(
            Duration.ofMinutes(5),      // 窗口大小
            Duration.ofMinutes(1)       // 容忍时间
        ).advanceBy(Duration.ofMinutes(2));  // 步长

        source
            .groupByKey()
            .windowedBy(windows)
            .count()
            .toStream()
            .to("hopping-window-output");
    }

    /**
     * 3. Sliding Window (滑动窗口)
     *
     * 特点:
     * - 用于 Join 操作
     * - 记录在窗口内则参与计算
     *
     * 时间线:
     * <---窗口大小---><--步长-->
     *  [记录A]--+
     *            +-- [窗口] -- [记录B]
     *            +-- [记录C]
     */
    public static void slidingWindow() {
        // 用于 Join 操作
        JoinWindows windows = JoinWindows.ofTimeDifferenceAndGrace(
            Duration.ofMinutes(5),      // 窗口大小
            Duration.ofMinutes(1)       // 容忍时间
        ).before(Duration.ofMinutes(2));  // 之前窗口大小

        /**
         * Sliding Window 主要用于:
         * - KStream Join
         * - KTable Join
         */
    }

    /**
     * 4. Session Window (会话窗口)
     *
     * 特点:
     * - 动态窗口大小
     * - 基于活动间隔
     * - 用于用户会话分析
     *
     * 时间线:
     * [会话1: 活跃]--空闲--[会话1: 继续]--空闲--[会话2: 新会话]
     */
    public static void sessionWindow(StreamsBuilder builder) {
        KStream<String, Long> source = builder.stream("input");

        // 创建会话窗口 (30分钟非活动间隔)
        SessionWindows windows = SessionWindows.with(Duration.ofMinutes(30));

        source
            .groupByKey()
            .windowedBy(windows)
            .count()
            .toStream()
            .to("session-window-output");
    }

    /**
     * 窗口操作示例
     */
    public static void windowOperations(StreamsBuilder builder) {
        KStream<String, Order> source = builder.stream("orders");

        // 1. 窗口聚合
        TimeWindows windowConfig = TimeWindows.ofSizeAndGrace(
            Duration.ofHours(1),        // 1小时窗口
            Duration.ofMinutes(5)       // 5分钟容忍
        );

        // 统计每小时的订单数
        source
            .groupBy((key, value) -> value.getProductId())
            .windowedBy(windowConfig)
            .count()
            .toStream()
            .foreach((key, count) -> {
                System.out.printf("Product: %s, Window: %s, Count: %d%n",
                    key.key(), key.window(), count);
            });

        // 2. 窗口内的最大值
        source
            .groupBy((key, value) -> value.getCategory())
            .windowedBy(windowConfig)
            .aggregate(
                () -> new OrderSummary(),
                (key, order, summary) -> {
                    if (order.getAmount() > summary.getMaxAmount()) {
                        summary.setMaxAmount(order.getAmount());
                        summary.setMaxOrderId(order.getOrderId());
                    }
                    return summary;
                },
                Materialized.as("max-order-store")
            );

        // 3. 早水印 (Early Emission)
        // 在窗口关闭前提前发出结果
        source
            .groupByKey()
            .windowedBy(windowConfig)
            .suppress(Suppressed.untilWindowCloses(
                Suppressed.BufferConfig.unbounded()
            ));
    }

    // 数据模型
    public static class Order {
        private String orderId;
        private String productId;
        private String category;
        private double amount;
        // getter/setter
    }

    public static class OrderSummary {
        private double maxAmount;
        private String maxOrderId;
        // getter/setter
    }
}
```

---

## 连接器

### 外部系统集成

```java
import org.apache.kafka.streams.*;

/**
 * Kafka Streams 连接器
 *
 * 1. Producer - 写入 Kafka
 * 2. Consumer - 读取 Kafka
 * 3. 自定义连接器 - 外部系统
 */
public class Connectors {

    /**
     * 1. 写入 Kafka Topic
     */
    public static void toKafka(StreamsBuilder builder) {
        KStream<String, String> source = builder.stream("input");

        // 方式1: 直接写入
        source.to("output-topic");

        // 方式2: 指定序列化器
        source.to("output-topic", Produced.with(
            Serdes.String(),
            Serdes.String()
        ));

        // 方式3: 条件路由
        source.selectKey((k, v) -> k)
            .to("output-topic");

        // 方式4: 分支路由
        KStream<String, String>[] branches = source.branch(
            (k, v) -> v.startsWith("A"),
            (k, v) -> v.startsWith("B"),
            (k, v) -> true
        );
        branches[0].to("topic-a");
        branches[1].to("topic-b");
        branches[2].to("topic-other");
    }

    /**
     * 2. 读取 Kafka Topic
     */
    public static void fromKafka() {
        StreamsBuilder builder = new StreamsBuilder();

        // 方式1: 读取单个 Topic
        KStream<String, String> stream1 = builder.stream("topic-1");

        // 方式2: 读取多个 Topic
        KStream<String, String> stream2 = builder.stream(
            java.util.Arrays.asList("topic-1", "topic-2")
        );

        // 方式3: 读取正则匹配的 Topic
        KStream<String, String> stream3 = builder.stream("topic-.*");

        // 方式4: 使用 Pattern
        KStream<String, String> stream4 = builder.stream(
            org.apache.kafka.common.serialization.Serdes.String(),
            org.apache.kafka.common.serialization.Serdes.String(),
            java.util.regex.Pattern.compile("orders-.*")
        );
    }

    /**
     * 3. 自定义 Sink (写入外部系统)
     */
    public static class CustomSinkExample {

        public static class MySinkTransformer
                implements Transformer<String, String, KeyValue<String, String>> {

            private org.apache.kafka.streams.processor.ProcessorContext context;

            @Override
            public void init(org.apache.kafka.streams.processor.ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<String, String> transform(String key, String value) {
                // 写入外部系统
                writeToExternalSystem(key, value);

                // 可以选择是否继续传递
                return KeyValue.pair(key, value);
            }

            @Override
            public void close() {
                // 关闭连接
            }

            private void writeToExternalSystem(String key, String value) {
                // 实现写入逻辑
            }
        }

        public static void useCustomSink(StreamsBuilder builder) {
            KStream<String, String> source = builder.stream("input");

            source.transform(MySinkTransformer::new)
                .to("output-topic");
        }
    }

    /**
     * 4. 自定义 Source (从外部系统读取)
     */
    public static class CustomSourceExample {

        public static class MySourceSupplier
                implements org.apache.kafka.streams.processor.internals.SourceNode {

            private org.apache.kafka.streams.processor.ProcessorContext context;

            @Override
            public void init(org.apache.kafka.streams.processor.ProcessorContext context) {
                this.context = context;
                // 开始从外部系统读取
                startReading();
            }

            @Override
            public void process(String key, String value) {
                // 不需要实现
            }

            @Override
            public KStreamBuilder process(KStreamBuilder builder) {
                return builder;
            }

            private void startReading() {
                // 从外部系统读取数据
                // context.forward(key, value);
            }
        }
    }
}
```

---

## 处理器拓扑

### 底层 API

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.*;

/**
 * Kafka Streams 处理器拓扑
 *
 * 两种构建方式:
 * 1. DSL API (推荐) - 使用 KStream/KTable API
 * 2. Processor API - 使用 Processor/Transformer
 */
public class ProcessorTopology {

    /**
     * 1. Processor API 示例
     */
    public static class ProcessorAPIExample {

        /**
         * 自定义处理器
         */
        public static class MyProcessor implements Processor<String, String> {

            private ProcessorContext context;
            private KeyValueStore<String, Integer> countStore;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                // 获取状态存储
                this.countStore = context.getStateStore("counts");
            }

            @Override
            public void process(String key, String value) {
                // 1. 解析输入
                if (value == null) return;

                // 2. 更新状态
                Integer count = countStore.get(key);
                if (count == null) count = 0;
                countStore.put(key, count + 1);

                // 3. 发送结果
                context.forward(key, count.toString());

                // 4. 提交偏移量
                context.commit();
            }

            @Override
            public void close() {
                // 清理资源
            }
        }

        /**
         * 自定义 Punctuator (定时处理器)
         */
        public static class MyPunctuator implements Punctuator {

            private KeyValueStore<String, Integer> countStore;
            private ProcessorContext context;

            public MyPunctuator(KeyValueStore<String, Integer> store,
                                ProcessorContext context) {
                this.countStore = store;
                this.context = context;
            }

            @Override
            public void punctuate(long timestamp) {
                // 定时执行的操作
                // 例如: 输出统计结果，清空计数器等
                System.out.println("定时统计触发");

                // 可以通过 forward 发送结果
                // context.forward("stats", "periodic-update");
            }
        }

        /**
         * 使用 Processor API 构建拓扑
         */
        public static void buildTopology() {
            StreamsBuilder builder = new StreamsBuilder();

            // 添加处理器
            builder.addProcessor(
                "my-processor",
                MyProcessor::new,
                "counts"  // 依赖的状态存储
            );

            // 添加状态存储
            builder.addStateStore(
                KeyValueStores.keyValueStoreBuilder(
                    org.apache.kafka.streams.state.Stores.persistentKeyValueStore("counts"),
                    org.apache.kafka.common.serialization.Serdes.String(),
                    org.apache.kafka.common.serialization.Serdes.Integer()
                )
            );

            // 连接节点
            builder.connectProcessorAndStateStores("my-processor", "counts");
        }
    }

    /**
     * 2. Transformer API 示例
     */
    public static class TransformerAPIExample {

        /**
         * 自定义 Transformer
         */
        public static class MyTransformer
                implements Transformer<String, String, KeyValue<String, String>> {

            private KeyValueStore<String, String> store;

            @Override
            public void init(ProcessorContext context) {
                this.store = context.getStateStore("transform-store");
            }

            @Override
            public KeyValue<String, String> transform(String key, String value) {
                // 转换逻辑
                String newKey = key.toUpperCase();
                String newValue = transformValue(value);

                // 存储状态
                store.put(newKey, newValue);

                // 可以输出多个结果
                context.forward("original", value);
                context.forward("transformed", KeyValue.pair(newKey, newValue));

                return KeyValue.pair(newKey, newValue);
            }

            private String transformValue(String value) {
                return value.toUpperCase();
            }

            @Override
            public void close() {}
        }

        /**
         * 自定义 ValueTransformer
         */
        public static class MyValueTransformer
                implements ValueTransformer<String, String> {

            @Override
            public void init(ProcessorContext context) {}

            @Override
            public String transform(String value) {
                return value.toUpperCase();
            }

            @Override
            public void close() {}
        }

        /**
         * 使用 Transformer
         */
        public static void useTransformer(StreamsBuilder builder) {
            KStream<String, String> source = builder.stream("input");

            // 转换并改变 Key
            source.transform(MyTransformer::new, "transform-store")
                .to("output");

            // 只转换 Value
            source.transformValues(MyValueTransformer::new)
                .to("output");
        }
    }

    /**
     * 3. 复杂拓扑示例
     */
    public static class ComplexTopology {

        /**
         * 构建复杂处理拓扑
         *
         * 输入: 订单流
         * 处理:
         * 1. 过滤有效订单
         * 2. 计算订单金额
         * 3. 更新统计
         * 4. 检测异常
         * 5. 输出到不同 Topic
         */
        public static void buildComplexTopology(StreamsBuilder builder) {
            KStream<String, Order> orders = builder.stream("orders");

            // 分支: 有效订单和无效订单
            KStream<String, Order>[] branches = orders.branch(
                (k, o) -> o.isValid(),       // 有效订单
                (k, o) -> !o.isValid()       // 无效订单
            );

            // 处理有效订单
            KStream<String, Order> validOrders = branches[0];
            KStream<String, Order> invalidOrders = branches[1];

            // 无效订单: 输出到告警 Topic
            invalidOrders
                .mapValues(o -> "INVALID: " + o.toString())
                .to("invalid-orders");

            // 有效订单: 统计和异常检测
            validOrders
                // 计算金额
                .mapValues(Order::calculateAmount)
                // 检测异常
                .transformValues(AnomalyDetector::new, "anomaly-store")
                // 分支: 正常和异常
                .branch(
                    (k, v) -> !v.isAnomaly(),
                    (k, v) -> v.isAnomaly()
                )
                .toArray()[1]  // 异常分支
                .mapValues(o -> "ANOMALY: " + o.toString())
                .to("order-anomalies");
        }

        public static class Order {
            private String orderId;
            private double amount;
            private boolean isValid;
            private boolean isAnomaly;

            public double calculateAmount() { return amount; }
            public boolean isValid() { return isValid; }
            public boolean isAnomaly() { return isAnomaly; }
        }

        public static class AnomalyDetector
                implements ValueTransformer<Order, Order> {

            @Override
            public Order transform(Order value) {
                // 异常检测逻辑
                if (value.getAmount() > 10000) {
                    value.setAnomaly(true);
                }
                return value;
            }

            @Override
            public void init(ProcessorContext context) {}

            @Override
            public void close() {}
        }
    }
}
```

---

## 最佳实践

### 生产环境配置

```java
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.common.serialization.Serdes;
import java.util.Properties;

/**
 * Kafka Streams 最佳实践
 */
public class StreamsBestPractices {

    /**
     * 生产环境配置
     */
    public static Properties productionConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "production-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            "kafka1:9092,kafka2:9092,kafka3:9092");

        // ============ 序列化配置 ============
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName());

        // ============ 处理配置 ============
        // 缓存大小
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024);

        // 提交间隔
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // 并行度
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        // 复制因子
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);

        // ============ 容错配置 ============
        // 故障处理
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
            StreamsConfig.EXACTLY_ONCE_V2);  // 精确一次

        // 恢复点
        props.put(StreamsConfig.CHANGELOG_TOPIC_REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/data/kafka-streams");

        // ============ 超时配置 ============
        props.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(StreamsConfig.POLL_TIMEOUT_MS_CONFIG, 1000);

        // ============ 容错处理 ============
        // 跳过无效记录
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            LogAndContinueExceptionHandler.class);

        return props;
    }

    /**
     * 容错处理
     */
    public static class FaultTolerance {

        /**
         * 状态监听器
         */
        public static void stateListener() {
            KafkaStreams streams = new KafkaStreams(builder, config);

            streams.setStateListener((newState, oldState) -> {
                System.out.printf("状态变更: %s -> %s%n", oldState, newState);

                switch (newState) {
                    case REBALANCING:
                        // 正在重新平衡，准备处理
                        prepareForRebalance();
                        break;
                    case RUNNING:
                        // 正常运行
                        onRunning();
                        break;
                    case PENDING_SHUTDOWN:
                        // 即将关闭
                        prepareForShutdown();
                        break;
                    case NOT_RUNNING:
                        // 已停止
                        onStopped();
                        break;
                }
            });
        }

        /**
         * 异常处理器
         */
        public static class CustomExceptionHandler
                implements org.apache.kafka.streams.errors.ExceptionHandler {

            @Override
            public ExceptionHandlerResponse handle(ProcessorContext context,
                                                    Throwable exception) {
                // 记录异常
                System.err.println("处理异常: " + exception.getMessage());

                // 根据异常类型处理
                if (exception instanceof IllegalArgumentException) {
                    // 参数错误，停止任务
                    return ExceptionHandlerResponse.FAIL;
                } else if (exception instanceof RuntimeException) {
                    // 运行时错误，重试
                    return ExceptionHandlerResponse.RETRY;
                } else {
                    // 其他错误，记录并继续
                    return ExceptionHandlerResponse.CONTINUE;
                }
            }
        }

        /**
         * 线程异常处理
         */
        public static void uncaughtExceptionHandler() {
            KafkaStreams streams = new KafkaStreams(builder, config);

            streams.setUncaughtExceptionHandler((thread, exception) -> {
                System.err.println("线程异常: " + thread.getName());
                exception.printStackTrace();

                // 重启 Streams
                streams.close();
                // 等待后重新启动
                try {
                    Thread.sleep(5000);
                    streams.start();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    /**
     * 性能优化
     */
    public static class PerformanceOptimization {

        public static Properties optimizedConfig() {
            Properties props = productionConfig();

            // 1. 增大缓存
            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG,
                64 * 1024 * 1024);  // 64MB

            // 2. 调整提交间隔
            // 频繁提交降低延迟，但增加开销
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

            // 3. 优化 RocksDB
            // 配置 RocksDB 参数
            // rocksdb.config.setter = com.example.RocksDBConfig

            // 4. 并行度
            // 根据 CPU 核心数设置
            props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                Runtime.getRuntime().availableProcessors());

            // 5. 缓冲区
            props.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, 1000);

            return props;
        }

        /**
         * RocksDB 配置
         */
        public static class RocksDBConfig
                implements org.apache.kafka.streams.state.RocksDBConfigSetter {

            @Override
            public void setConfig(String storeName,
                                  org.rocksdb.Options options,
                                  Map<String, Object> configs) {
                // 优化 RocksDB 配置
                options.setWriteBufferSize(64 * 1024 * 1024);  // 64MB
                options.setMaxWriteBufferNumber(3);
                options.setTargetFileSizeBase(64 * 1024 * 1024);  // 64MB
                options.setMaxBackgroundCompactions(4);
            }
        }
    }

    /**
     * 监控指标
     */
    public static void monitoring() {
        KafkaStreams streams = new KafkaStreams(builder, config);

        // 获取指标
        KafkaMetrics metrics = new KafkaMetrics(streams);

        // 关键指标
        // alive-stream-threads: 存活线程数
        // commit-rate: 提交速率
        // poll-rate: 拉取速率
        // process-rate: 处理速率
        // punctuate-rate: 定时触发速率
        // task-created-rate: 任务创建速率

        // 使用 JMX
        // jmx.opentsdb.host.override = localhost:2003

        streams.close();
    }

    // 辅助方法
    private static void prepareForRebalance() {}
    private static void onRunning() {}
    private static void prepareForShutdown() {}
    private static void onStopped() {}
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Kafka 架构详解](01-architecture.md) | Kafka 核心架构和概念 |
| [Kafka Producer 指南](02-producer.md) | 生产者 API 和配置详解 |
| [Kafka Consumer 指南](03-consumer.md) | 消费者 API 和配置详解 |
| [Kafka 运维指南](05-operations.md) | 集群部署和运维 |
| [Kafka 故障排查](06-troubleshooting.md) | 常见问题和解决方案 |
