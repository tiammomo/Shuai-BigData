package com.bigdata.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Kafka Producer/Consumer API Examples
 */
public class KafkaExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "test-topic";

    // ==================== Producer Examples ====================

    /**
     * 简单同步Producer
     */
    public static void simpleProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                String key = "key-" + i;
                String value = "message-" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                // 同步发送
                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata metadata = future.get();
                System.out.printf("Sent to partition %d, offset %d: %s%n",
                        metadata.partition(), metadata.offset(), value);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步Producer with Callback
     */
    public static void asyncProducerWithCallback() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // 幂等发送

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "message-" + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

            // 异步发送带回调
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Failed to send message: " + exception.getMessage());
                } else {
                    System.out.printf("Sent to partition %d, offset %d: %s%n",
                            metadata.partition(), metadata.offset(), value);
                }
            });
        }

        // 等待所有发送完成
        producer.flush();
        producer.close();
    }

    /**
     * 发送JSON消息
     */
    public static void jsonProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        List<Map<String, Object>> messages = new ArrayList<>();
        messages.add(createOrderMessage(1L, "Alice", 100.0));
        messages.add(createOrderMessage(2L, "Bob", 200.0));
        messages.add(createOrderMessage(3L, "Charlie", 300.0));

        for (Map<String, Object> msg : messages) {
            String json = com.alibaba.fastjson.JSON.toJSONString(msg);
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, json);
            producer.send(record);
        }

        producer.flush();
        producer.close();
    }

    /**
     * 指定分区发送
     */
    public static void sendToPartition() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送到指定分区
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, 0, "key", "value");
        producer.send(record);

        producer.flush();
        producer.close();
    }

    // ==================== Consumer Examples ====================

    /**
     * 简单Consumer
     */
    public static void simpleConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 手动提交offset
     */
    public static void manualCommitConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-commit-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // 关闭自动提交

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // 处理消息
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Processing: " + record.value());
                }

                // 手动提交offset
                consumer.commitAsync();
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 消费JSON消息
     */
    public static void jsonConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // 解析JSON
                    Map<String, Object> order = com.alibaba.fastjson.JSON.parseObject(
                            record.value(), Map.class);
                    System.out.println("Order: " + order);
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 指定分区消费
     */
    public static void consumeFromPartition() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "partition-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅指定分区
        consumer.assign(Collections.singletonList(
                new org.apache.kafka.common.TopicPartition(TOPIC, 0)));

        // 从指定offset开始消费
        consumer.seek(new org.apache.kafka.common.TopicPartition(TOPIC, 0), 100L);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Offset: " + record.offset() + ", Value: " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    // ==================== Admin API Examples ====================

    /**
     * 创建Topic
     */
    public static void createTopic() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Topic配置
            Map<String, String> configs = new HashMap<>();
            configs.put("retention.ms", "604800000"); // 7天

            NewTopic topic = new NewTopic("my-topic", 3, (short) 1);
            topic.configs(configs);

            adminClient.createTopics(Collections.singletonList(topic))
                    .all()
                    .get();
            System.out.println("Topic created successfully");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列出所有Topic
     */
    public static void listTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult result = adminClient.listTopics();
            Set<String> topics = result.names().get();
            topics.forEach(System.out::println);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    // ==================== 辅助方法 ====================

    private static Map<String, Object> createOrderMessage(Long orderId, String userId, Double amount) {
        Map<String, Object> order = new HashMap<>();
        order.put("order_id", orderId);
        order.put("user_id", userId);
        order.put("amount", amount);
        order.put("timestamp", System.currentTimeMillis());
        return order;
    }

    public static void main(String[] args) {
        // 运行示例
        // asyncProducerWithCallback();
        // simpleConsumer();
    }
}
