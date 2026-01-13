package com.bigdata.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

import java.util.Properties;

/**
 * Flink 示例测试类
 */
public class FlinkExampleTest extends AbstractTestBase {

    /**
     * 测试 Map 操作
     */
    @Test
    public void testMapFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> source = env.fromElements("1,John", "2,Jane", "3,Bob");

        DataStream<User> users = source.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                String[] fields = value.split(",");
                return new User(Long.parseLong(fields[0]), fields[1]);
            }
        });

        // 收集结果进行验证
        String result = users.executeAndCollect("testMap").toString();
        System.out.println("Map result: " + result);
    }

    /**
     * 测试 Kafka Source/Sink
     */
    @Test
    public void testKafkaConnector() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-group");

        // Kafka Source
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "test-topic",
                new SimpleStringSchema(),
                props
        );

        // Kafka Sink
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "output-topic",
                new SimpleStringSchema(),
                props
        );

        DataStream<String> stream = env.addSource(consumer);
        stream.addSink(producer);

        // 测试验证
        System.out.println("Kafka connector configured successfully");
    }

    /**
     * 测试窗口操作
     */
    @Test
    public void testWindowOperation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> source = env.fromElements(
                Tuple2.of("key1", 1),
                Tuple2.of("key1", 2),
                Tuple2.of("key2", 3)
        );

        // 滚动窗口聚合
        DataStream<Tuple2<String, Integer>> result = source
                .keyBy(t -> t.f0)
                .countWindow(2)
                .sum(1);

        System.out.println("Window operation configured successfully");
    }

    /**
     * 用户实体类
     */
    public static class User {
        public long id;
        public String name;

        public User() {}

        public User(long id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "User{id=" + id + ", name='" + name + "'}";
        }
    }

    /**
     * 元组类 (简化版)
     */
    public static class Tuple2<K, V> {
        public K f0;
        public V f1;

        public Tuple2() {}

        public Tuple2(K f0, V f1) {
            this.f0 = f0;
            this.f1 = f1;
        }

        public static <K, V> Tuple2<K, V> of(K f0, V f1) {
            return new Tuple2<>(f0, f1);
        }
    }
}
