package com.bigdata.example;

/**
 * BigData Examples Application Entry Point
 *
 * This project provides examples for:
 * - Spark: RDD, SQL, Streaming, Hive, Iceberg integration
 * - Flink: DataStream, Table API, SQL, Kafka integration
 * - Kafka: Producer, Consumer, Streams
 * - Doris: JDBC, Stream Load, Spark/Flink integration
 * - ZooKeeper: Curator framework, distributed coordination
 * - HBase: CRUD, filters, Spark integration
 * - Elasticsearch: CRUD, aggregations, Spark/Flink integration
 * - Redis: String, List, Set, Hash, SortedSet operations
 * - Flume: SDK, configuration, custom sink/source
 * - Paimon: Stream-batch unified data lake, CDC sync
 */
public class BigDataExampleApplication {

    public static void main(String[] args) {
        System.out.println("==============================================");
        System.out.println("    BigData Examples Application");
        System.out.println("==============================================");
        System.out.println();
        System.out.println("Available Examples:");
        System.out.println();
        System.out.println("  Spark Examples:");
        System.out.println("    - com.bigdata.example.spark.SparkRDDExample");
        System.out.println("    - com.bigdata.example.spark.SparkSQLExample");
        System.out.println("    - com.bigdata.example.spark.SparkKafkaExample");
        System.out.println("    - com.bigdata.example.spark.SparkHiveExample");
        System.out.println("    - com.bigdata.example.spark.SparkIcebergExample");
        System.out.println("    - com.bigdata.example.spark.SparkPaimonExample");
        System.out.println();
        System.out.println("  Flink Examples:");
        System.out.println("    - com.bigdata.example.flink.FlinkDataStreamExample");
        System.out.println("    - com.bigdata.example.flink.FlinkTableAPIExample");
        System.out.println("    - com.bigdata.example.flink.FlinkKafkaExample");
        System.out.println("    - com.bigdata.example.flink.FlinkJDBCExample");
        System.out.println("    - com.bigdata.example.flink.FlinkIcebergExample");
        System.out.println("    - com.bigdata.example.flink.FlinkPaimonExample");
        System.out.println("    - com.bigdata.example.flink.FlinkStatefulProcessing");
        System.out.println("    - com.bigdata.example.flink.FlinkMultiStreamJoin");
        System.out.println("    - com.bigdata.example.flink.FlinkDataStreamApi");
        System.out.println("    - com.bigdata.example.flink.FlinkAdvancedFeatures");
        System.out.println();
        System.out.println("  Kafka Examples:");
        System.out.println("    - com.bigdata.example.kafka.KafkaExample");
        System.out.println("    - com.bigdata.example.kafka.KafkaStreamsExample");
        System.out.println();
        System.out.println("  Doris Examples:");
        System.out.println("    - com.bigdata.example.doris.DorisJDBCExample");
        System.out.println("    - com.bigdata.example.doris.DorisStreamLoadExample");
        System.out.println("    - com.bigdata.example.doris.DorisSparkExample");
        System.out.println("    - com.bigdata.example.doris.DorisFlinkExample");
        System.out.println("    - com.bigdata.example.doris.DorisBitmapExample");
        System.out.println();
        System.out.println("  Iceberg Examples:");
        System.out.println("    - com.bigdata.example.spark.SparkIcebergExample");
        System.out.println("    - com.bigdata.example.flink.FlinkIcebergExample");
        System.out.println();
        System.out.println("  Paimon Examples:");
        System.out.println("    - com.bigdata.example.spark.SparkPaimonExample");
        System.out.println("    - com.bigdata.example.flink.FlinkPaimonExample");
        System.out.println();
        System.out.println("  ZooKeeper Examples:");
        System.out.println("    - com.bigdata.example.zookeeper.ZookeeperExample");
        System.out.println("    - com.bigdata.example.zookeeper.ZookeeperRecipesExample");
        System.out.println();
        System.out.println("  HBase Examples:");
        System.out.println("    - com.bigdata.example.hbase.HBaseExample");
        System.out.println("    - com.bigdata.example.hbase.SparkHBaseExample");
        System.out.println();
        System.out.println("  Elasticsearch Examples:");
        System.out.println("    - com.bigdata.example.elasticsearch.ElasticsearchExample");
        System.out.println("    - com.bigdata.example.elasticsearch.SparkElasticsearchExample");
        System.out.println("    - com.bigdata.example.elasticsearch.FlinkElasticsearchExample");
        System.out.println();
        System.out.println("  Redis Examples:");
        System.out.println("    - com.bigdata.example.redis.RedisExample");
        System.out.println();
        System.out.println("  Flume Examples:");
        System.out.println("    - com.bigdata.example.flume.FlumeExample");
        System.out.println("    - com.bigdata.example.flume.FlumeConfigurationExample");
        System.out.println();
        System.out.println("==============================================");

        // 根据参数运行对应的示例
        if (args.length > 0) {
            String example = args[0];
            try {
                Class<?> clazz = Class.forName("com.bigdata.example." + example);
                clazz.getMethod("main", String[].class).invoke(null, (Object) args);
            } catch (ClassNotFoundException e) {
                System.err.println("Example not found: " + example);
            } catch (Exception e) {
                System.err.println("Error running example: " + e.getMessage());
            }
        }
    }
}
