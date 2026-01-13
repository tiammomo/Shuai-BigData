package com.bigdata.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Properties;

/**
 * Flink Kafka Integration Examples
 */
public class FlinkKafkaExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // ==================== Kafka Source ====================

        // 1. 从Kafka读取 (FlinkKafkaConsumer)

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");
        consumerProps.setProperty("group.id", "flink-consumer-group");

        // 方式1: 简单字符串
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "input-topic",
                new SimpleStringSchema(),
                consumerProps
        );
        consumer.setStartFromEarliest();
        consumer.setCommitOffsetsOnCheckpoints(true);

        DataStream<String> kafkaStream = env.addSource(consumer);

        // 方式2: 自定义反序列化
        FlinkKafkaConsumer<MyEvent> eventConsumer = new FlinkKafkaConsumer<>(
                "events-topic",
                new MyEventDeserializationSchema(),
                consumerProps
        );

        // 方式3: JSON格式
        // 需要引入 flink-json 或使用 GenericTypeInfo

        // 方式4: 从Checkpoint恢复
        // consumer.setStartFromGroupOffsets();

        // ==================== Kafka Sink ====================

        // 1. 写出到Kafka (FlinkKafkaProducer)

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "output-topic",
                new SimpleStringSchema(),
                producerProps
        );

        kafkaStream.addSink(producer);

        // 2. 带精确一次语义的Producer
        FlinkKafkaProducer<String> exactlyOnceProducer = new FlinkKafkaProducer<>(
                "output-topic",
                new SimpleStringSchema(),
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        // ==================== 业务处理示例 ====================

        // 1. 解析JSON消息
        DataStream<Tuple2<String, Integer>> wordCounts = kafkaStream
                .flatMap((line, out) -> {
                    for (String word : line.split("\\s+")) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);

        // 2. 写出处理结果
        wordCounts.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0 + ":" + value.f1;
            }
        }).addSink(exactlyOnceProducer);

        // ==================== Table API/SQL ====================

        org.apache.flink.streaming.api.environment.StreamTableEnvironment tableEnv =
                org.apache.flink.streaming.api.environment.StreamTableEnvironment.create(env);

        // 创建Source表
        tableEnv.executeSql(
                "CREATE TABLE kafka_source (" +
                "  `user_id` BIGINT," +
                "  `item_id` BIGINT," +
                "  `behavior` STRING," +
                "  `ts` TIMESTAMP(3)," +
                "  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'user-behavior'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'flink-sql-group'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")"
        );

        // 创建Sink表
        tableEnv.executeSql(
                "CREATE TABLE kafka_sink (" +
                "  `user_id` BIGINT," +
                "  `behavior_count` BIGINT" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'behavior-count'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'format' = 'json'" +
                ")"
        );

        // 执行实时聚合
        tableEnv.executeSql(
                "INSERT INTO kafka_sink " +
                "SELECT user_id, COUNT(*) as behavior_count " +
                "FROM kafka_source " +
                "GROUP BY user_id"
        );

        env.execute("Flink Kafka Example");
    }

    /**
     * 自定义事件类
     */
    public static class MyEvent {
        public long userId;
        public String eventType;
        public long timestamp;

        public MyEvent() {}

        public MyEvent(long userId, String eventType, long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }
    }

    /**
     * 自定义反序列化器
     */
    public static class MyEventDeserializationSchema
            implements KafkaDeserializationSchema<MyEvent> {

        @Override
        public boolean isEndOfStream(MyEvent nextElement) {
            return false;
        }

        @Override
        public MyEvent deserialize(
                org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]> record) throws Exception {
            String json = new String(record.value());
            // 使用FastJSON解析
            com.alibaba.fastjson.JSONObject obj = com.alibaba.fastjson.JSON.parseObject(json);
            MyEvent event = new MyEvent();
            event.userId = obj.getLong("user_id");
            event.eventType = obj.getString("event_type");
            event.timestamp = obj.getLong("timestamp");
            return event;
        }

        @Override
        public org.apache.flink.api.common.typeinfo.TypeInfo<MyEvent> getProducedType() {
            return org.apache.flink.api.common.typeinfo.TypeInfo.of(MyEvent.class);
        }
    }
}
