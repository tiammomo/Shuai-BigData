package com.bigdata.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Kafka Streams API Examples - 流处理框架
 */
public class KafkaStreamsExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String INPUT_TOPIC = "text-input";
    private static final String OUTPUT_TOPIC = "word-count-output";
    private static final String STORE_NAME = "word-count-store";

    /**
     * 1. 简单WordCount示例
     */
    public static void wordCountExample() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // 从Kafka读取
        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        // 转换: 分割成单词
        KStream<String, String> words = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\s+")));

        // 按单词分组并计数
        KTable<String, Long> counts = words
                .groupBy((key, value) -> KeyValue.pair(value, value))
                .count(Materialized.as(STORE_NAME));

        // 写出到Kafka
        counts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        // 构建拓扑
        Topology topology = builder.build();
        System.out.println(topology.describe());

        // 启动流处理
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }

    /**
     * 2. 带有窗口的WordCount
     */
    public static void windowedWordCountExample() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(INPUT_TOPIC);

        // 滚动窗口 (1分钟)
        KTable<Windowed<String>, Long> windowedCounts = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\s+")))
                .groupBy((key, value) -> KeyValue.pair(value, value))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count();

        // 转换为普通KTable输出
        windowedCounts.toStream()
                .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key() + "@" + windowedKey.window().start(),
                        count))
                .to(OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    /**
     * 3. 连接操作 (Stream-Stream Join)
     */
    public static void streamJoinExample() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        StreamsBuilder builder = new StreamsBuilder();

        // 两个流
        KStream<String, String> orders = builder.stream("orders-topic");
        KStream<String, String> shipments = builder.stream("shipments-topic");

        // Stream-Stream Join (5分钟窗口内连接)
        KStream<String, String> joined = orders.join(
                shipments,
                (orderValue, shipmentValue) -> "order=" + orderValue + ", shipment=" + shipmentValue,
                JoinWindows.of(Duration.ofMinutes(5))
        );

        joined.to("order-shipment-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    /**
     * 4. Stream-Table Join (Table作为维表)
     */
    public static void streamTableJoinExample() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        StreamsBuilder builder = new StreamsBuilder();

        // 创建Table (用户维度表)
        KTable<String, String> usersTable = builder.table("users-topic");

        // 创建Stream (订单流)
        KStream<String, String> ordersStream = builder.stream("orders-topic");

        // Stream-Table Join
        KStream<String, String> enrichedOrders = ordersStream
                .join(usersTable,
                        (orderKey, orderValue) -> orderKey,
                        (orderValue, userValue) -> orderValue + " | user=" + userValue);

        enrichedOrders.to("enriched-orders-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    /**
     * 5. 聚合操作
     */
    public static void aggregationExample() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregation-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        StreamsBuilder builder = new StreamsBuilder();

        // 自定义状态存储
        StoreBuilder<KeyValueStore<String, Long>> countStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("counts"),
                        Serdes.String(),
                        Serdes.Long()
                );

        builder.addStateStore(countStore);

        KStream<String, String> source = builder.stream("events-topic");

        // 使用自定义聚合
        KTable<String, Long> aggregated = source
                .groupByKey()
                .aggregate(
                        () -> 0L,
                        (key, value, aggregate) -> aggregate + 1,
                        Materialized.as("counts")
                );

        aggregated.toStream().to("aggregated-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    /**
     * 6. 过滤和转换
     */
    public static void transformExample() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "transform-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("raw-topic");

        // 过滤
        KStream<String, String> filtered = source.filter(
                (key, value) -> value.length() > 10
        );

        // 映射
        KStream<String, String> mapped = filtered.mapValues(
                value -> value.toUpperCase()
        );

        // FlatMap (可以改变key)
        KStream<String, String> flatMapped = mapped.flatMap(
                (key, value) -> {
                    List<KeyValue<String, String>> result = new ArrayList<>();
                    for (String word : value.split(" ")) {
                        result.add(KeyValue.pair(word, word));
                    }
                    return result;
                }
        );

        flatMapped.to("processed-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    /**
     * 7. 分支(Branch)
     */
    public static void branchExample() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "branch-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("events-topic");

        // 分支
        KStream<String, String>[] branches = source.branch(
                (key, value) -> value.startsWith("error"),
                (key, value) -> value.startsWith("warn"),
                (key, value) -> true // 默认
        );

        branches[0].to("errors-topic");      // 错误日志
        branches[1].to("warnings-topic");    // 警告日志
        branches[2].to("infos-topic");       // 信息日志

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public static void main(String[] args) {
        // 启动流处理应用
        // wordCountExample();
        // windowedWordCountExample();
    }
}
