package com.bigdata.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

/**
 * Spark Streaming + Kafka Integration Examples
 */
public class SparkKafkaExample {

    public static void main(String[] args) throws TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("SparkKafkaExample")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", 4)
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        try {
            // 1. 从Kafka读取数据 (Structured Streaming)
            Dataset<Row> df = spark
                    .readStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("subscribe", "test-topic")
                    .option("startingOffsets", "latest")
                    .option("failOnDataLoss", "false")
                    .load();

            // 2. 解析Kafka消息
            Dataset<Row> parsed = df.select(
                    col("key").cast("string"),
                    col("value").cast("string"),
                    col("partition"),
                    col("offset"),
                    col("timestamp")
            );

            // 3. 简单处理：统计单词数量
            Dataset<Row> wordCounts = parsed
                    .selectExpr("explode(split(value, ' ')) as word")
                    .groupBy("word")
                    .count();

            // 4. 写出到Kafka
            String outputTopic = "output-topic";
            wordCounts.select(
                    col("word").cast("string").alias("key"),
                    col("count").cast("string").alias("value")
                    )
                    .writeStream()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "localhost:9092")
                    .option("topic", outputTopic)
                    .option("checkpointLocation", "hdfs://path/to/checkpoint")
                    .outputMode(OutputMode.Update())
                    .start()
                    .awaitTermination();

        } finally {
            spark.close();
        }
    }

    /**
     * Spark Streaming (DStream) 方式读取Kafka
     */
    public static void sparkStreamingKafkaExample() {
        SparkSession spark = SparkSession.builder()
                .appName("SparkStreamingKafkaExample")
                .master("local[*]")
                .getOrCreate();

        // 注意：DStream方式需要使用JavaStreamingContext
        // 详见 org.apache.spark.streaming.api.java.JavaStreamingContext

        spark.close();
    }
}
