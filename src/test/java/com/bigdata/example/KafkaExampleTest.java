package com.bigdata.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 * Kafka 示例测试类
 */
public class KafkaExampleTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TEST_TOPIC = "test-topic";

    /**
     * 测试生产者配置
     */
    @Test
    public void testProducerConfig() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 验证配置
        assertNotNull(producer);
        assertEquals(BOOTSTRAP_SERVERS, producer.getConfig().configs().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        producer.close();
    }

    /**
     * 测试消费者配置
     */
    @Test
    public void testConsumerConfig() {
        String groupId = "test-group";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 验证配置
        assertNotNull(consumer);
        assertEquals(groupId, consumer.getConfig().groupId());

        consumer.close();
    }

    /**
     * 测试生产者发送消息
     */
    @Test
    public void testProducerSend() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TEST_TOPIC, "key", "value");

            // 发送消息
            var future = producer.send(record);
            var metadata = future.get();

            assertEquals(TEST_TOPIC, metadata.topic());
            assertNotNull(metadata.offset());
        }
    }

    /**
     * 测试消费者接收消息
     */
    @Test
    public void testConsumerPoll() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-poll-group");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TEST_TOPIC));

            // 轮询消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            // 验证可以正常轮询
            assertNotNull(records);
        }
    }

    /**
     * 测试 AdminClient 配置
     */
    @Test
    public void testAdminClientConfig() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult topics = adminClient.listTopics();
            var names = topics.names().get();

            // 验证可以获取 Topic 列表
            assertNotNull(names);
        }
    }
}
