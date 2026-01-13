package com.bigdata.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * Flink + Elasticsearch Integration Examples
 */
public class FlinkElasticsearchExample {

    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;
    private static final String ES_INDEX = "flink_es_sink";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== Flink -> ES ====================

        // 1. 创建Flink源表
        tableEnv.executeSql(
                "CREATE TABLE flink_source (" +
                "  id BIGINT," +
                "  name STRING," +
                "  amount DOUBLE," +
                "  timestamp TIMESTAMP(3)," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:3306/db'," +
                "  'table-name' = 'orders'," +
                "  'username' = 'root'," +
                "  'password' = ''" +
                ")"
        );

        // 2. 创建ES Sink表
        tableEnv.executeSql(
                "CREATE TABLE es_sink (" +
                "  id BIGINT," +
                "  name STRING," +
                "  amount DOUBLE," +
                "  insert_time TIMESTAMP(3)," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'elasticsearch-8'," +
                "  'hosts' = 'http://" + ES_HOST + ":" + ES_PORT + "'," +
                "  'index' = '" + ES_INDEX + "'," +
                "  'sink.bulk-flush.max-actions' = '1000'," +
                "  'sink.bulk-flush.max-size' = '2mb'," +
                "  'sink.bulk-flush.interval' = '1s'" +
                ")"
        );

        // 3. 从MySQL读取，实时写入ES
        tableEnv.executeSql(
                "INSERT INTO es_sink " +
                "SELECT id, name, amount, CURRENT_TIMESTAMP " +
                "FROM flink_source"
        );

        // ==================== ES -> Flink (Lookup) ====================

        // 4. 创建ES维表 (Lookup Join)
        tableEnv.executeSql(
                "CREATE TABLE es_lookup (" +
                "  user_id BIGINT," +
                "  user_name STRING," +
                "  email STRING," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'elasticsearch-8'," +
                "  'hosts' = 'http://" + ES_HOST + ":" + ES_PORT + "'," +
                "  'index' = 'users'," +
                "  'lookup.cache.max-rows' = '1000'," +
                "  'lookup.cache.ttl' = '1h'," +
                "  'lookup.max-retries' = '3'" +
                ")"
        );

        // 5. Kafka流关联ES维表
        tableEnv.executeSql(
                "CREATE TABLE kafka_stream (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  amount DOUBLE," +
                "  order_time TIMESTAMP(3)," +
                "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'orders'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'flink-group'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")"
        );

        Table enrichedOrders = tableEnv.sqlQuery(
                "SELECT o.order_id, o.user_id, l.user_name, l.email, o.amount " +
                "FROM kafka_stream o " +
                "JOIN es_lookup FOR SYSTEM_TIME AS OF o.proc_time AS l " +
                "ON o.user_id = l.user_id"
        );

        enrichedOrders.execute().print();

        // ==================== 批量处理 ====================

        // 6. 批处理 - 读取ES数据
        tableEnv.executeSql(
                "CREATE TABLE es_batch_source (" +
                "  id BIGINT," +
                "  name STRING," +
                "  amount DOUBLE," +
                "  ts TIMESTAMP(3)" +
                ") WITH (" +
                "  'connector' = 'elasticsearch-8'," +
                "  'hosts' = 'http://" + ES_HOST + ":" + ES_PORT + "'," +
                "  'index' = '" + ES_INDEX + "'," +
                "  'scan.mode' = 'batch'" +
                ")"
        );

        // 批处理查询
        Table batchResult = tableEnv.sqlQuery(
                "SELECT name, SUM(amount) as total_amount, COUNT(*) as cnt " +
                "FROM es_batch_source " +
                "GROUP BY name " +
                "ORDER BY total_amount DESC"
        );

        batchResult.execute().print();

        // ==================== ES Sink详细配置 ====================

        // 7. 使用动态索引
        tableEnv.executeSql(
                "CREATE TABLE es_dynamic_sink (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  amount DOUBLE," +
                "  order_date STRING" +
                ") WITH (" +
                "  'connector' = 'elasticsearch-8'," +
                "  'hosts' = 'http://" + ES_HOST + ":" + ES_PORT + "'," +
                "  'index' = 'orders-${order_date}'," +
                "  'sink.bulk-flush.max-actions' = '1000'" +
                ")"
        );

        // 8. 路由配置
        tableEnv.executeSql(
                "CREATE TABLE es_routing_sink (" +
                "  id BIGINT," +
                "  name STRING," +
                "  routing STRING" +
                ") WITH (" +
                "  'connector' = 'elasticsearch-8'," +
                "  'hosts' = 'http://" + ES_HOST + ":" + ES_PORT + "'," +
                "  'index' = 'routed_index'," +
                "  'sink.bulk-flush.max-actions' = '500'" +
                ")"
        );

        // 9. 错误处理配置
        tableEnv.executeSql(
                "CREATE TABLE es_error_handling_sink (" +
                "  id BIGINT," +
                "  data STRING" +
                ") WITH (" +
                "  'connector' = 'elasticsearch-8'," +
                "  'hosts' = 'http://" + ES_HOST + ":" + ES_PORT + "'," +
                "  'index' = 'error_logs'," +
                "  'sink.bulk-flush.max-retries' = '3'," +
                "  'sink.bulk-flush.retry-interval' = '10s'" +
                ")"
        );

        // ==================== DataStream API ====================

        // 10. DataStream写入ES
        DataStreamAPIExample(env);

        System.out.println("Flink Elasticsearch Examples Completed!");
    }

    /**
     * DataStream API写入ES
     */
    private static void DataStreamAPIExample(StreamExecutionEnvironment env) {
        // 注意: DataStream API需要使用Elasticsearch Sink Function
        // 详见 org.apache.flink.streaming.connectors.elasticsearch

        // 示例配置:
        // List<HttpHost> httpHosts = new ArrayList<>();
        // httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        //
        // ElasticsearchSink.Builder<String> esSinkBuilder =
        //         new ElasticsearchSink.Builder<>(
        //                 httpHosts,
        //                 new ElasticsearchSinkFunction<String>() {
        //                     @Override
        //                     public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        //                         Map<String, String> json = new HashMap<>();
        //                         json.put("data", element);
        //
        //                         IndexRequest request = Requests.indexRequest()
        //                                 .index("my-index")
        //                                 .source(json);
        //
        //                         indexer.add(request);
        //                     }
        //                 }
        //         );
        //
        // esSinkBuilder.setBulkFlushMaxActions(1000);
        // esSinkBuilder.setBulkFlushMaxSizeMb(5);
        // esSinkBuilder.setBulkFlushInterval(5000);
        //
        // env.fromElements("data1", "data2")
        //    .addSink(esSinkBuilder.build());
    }

    /**
     * ES文档POJO
     */
    public static class OrderEvent {
        public long orderId;
        public String userId;
        public double amount;
        public LocalDateTime orderTime;

        public OrderEvent() {}

        public OrderEvent(long orderId, String userId, double amount) {
            this.orderId = orderId;
            this.userId = userId;
            this.amount = amount;
            this.orderTime = LocalDateTime.now();
        }
    }
}
