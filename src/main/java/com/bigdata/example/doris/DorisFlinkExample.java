package com.bigdata.example.doris;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * Flink + Doris Integration Examples
 */
public class DorisFlinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== Doris作为Source ====================

        // 1. 批量读取Doris (Lookup Source)
        tableEnv.executeSql(
                "CREATE TABLE doris_source (" +
                "  id BIGINT," +
                "  name STRING," +
                "  age INT," +
                "  score DOUBLE," +
                "  create_time TIMESTAMP(3)" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:9030/test_db'," +
                "  'table-name' = 'test_table'," +
                "  'username' = 'root'," +
                "  'password' = ''" +
                ")"
        );

        // 2. 实时流式读取Doris
        tableEnv.executeSql(
                "CREATE TABLE doris_stream_source (" +
                "  id BIGINT," +
                "  name STRING," +
                "  amount DOUBLE," +
                "  event_time TIMESTAMP(3)," +
                "  proc_time AS PROCTIME()" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:9030/test_db'," +
                "  'table-name' = 'orders'," +
                "  'username' = 'root'," +
                "  'password' = ''" +
                "  -- 'lookup.cache.max-rows' = '1000'," +
                "  -- 'lookup.cache.ttl' = '1h'" +
                ")"
        );

        // ==================== Doris作为Sink ====================

        // 3. 批量写入Doris
        tableEnv.executeSql(
                "CREATE TABLE doris_sink (" +
                "  id BIGINT," +
                "  name STRING," +
                "  total_amount DOUBLE," +
                "  order_count BIGINT," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:9030/test_db'," +
                "  'table-name' = 'user_stats'," +
                "  'username' = 'root'," +
                "  'password' = ''" +
                "  -- 'sink.buffer-flush.max-rows' = '1000'," +
                "  -- 'sink.buffer-flush.interval' = '1m'," +
                "  -- 'sink.max-retries' = '3'" +
                ")"
        );

        // 4. 实时写入Doris (支持upsert)
        tableEnv.executeSql(
                "CREATE TABLE doris_upsert_sink (" +
                "  user_id BIGINT," +
                "  pv BIGINT," +
                "  uv BIGINT," +
                "  last_update TIMESTAMP(3)," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:9030/test_db'," +
                "  'table-name' = 'user_behavior_stats'," +
                "  'username' = 'root'," +
                "  'password' = ''," +
                "  'sink.buffer-flush.max-rows' = '5000'," +
                "  'sink.buffer-flush.interval' = '10s'," +
                "  'sink.max-retries' = '5'" +
                ")"
        );

        // ==================== Flink SQL操作 ====================

        // 5. 简单查询
        Table dorisTable = tableEnv.from("doris_source");
        Table filtered = dorisTable.filter($("age").isGreater(25));
        filtered.execute().print();

        // 6. 聚合查询
        Table aggregated = tableEnv.sqlQuery(
                "SELECT name, COUNT(*) as cnt, AVG(score) as avg_score " +
                "FROM doris_source " +
                "GROUP BY name"
        );

        // 7. 写入Doris
        aggregated.insertInto("doris_sink");
        tableEnv.execute("Insert into Doris");

        // 8. 实时聚合写入
        tableEnv.executeSql(
                "INSERT INTO doris_upsert_sink " +
                "SELECT user_id, COUNT(*) as pv, COUNT(DISTINCT device_id) as uv, " +
                "       CURRENT_TIMESTAMP as last_update " +
                "FROM kafka_source " +
                "GROUP BY user_id"
        );

        // ==================== 维表查询 (Lookup Join) ====================

        // Doris作为维表进行关联查询
        tableEnv.executeSql(
                "CREATE TABLE dim_user (" +
                "  user_id BIGINT," +
                "  user_name STRING," +
                "  gender STRING," +
                "  age INT," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:9030/test_db'," +
                "  'table-name' = 'dim_user'," +
                "  'username' = 'root'," +
                "  'password' = ''" +
                ")"
        );

        // Kafka流关联Doris维表
        tableEnv.executeSql(
                "CREATE TABLE kafka_source (" +
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

        // 维表关联
        Table enrichedOrders = tableEnv.sqlQuery(
                "SELECT o.order_id, o.user_id, d.user_name, d.gender, o.amount " +
                "FROM kafka_source o " +
                "JOIN dim_user FOR SYSTEM_TIME AS OF o.proc_time AS d " +
                "ON o.user_id = d.user_id"
        );

        enrichedOrders.execute().print();

        // ==================== Doris到Flink再到Doris (ETL) ====================

        // 9. Doris -> Flink -> Doris
        tableEnv.executeSql(
                "INSERT INTO doris_sink " +
                "SELECT id, name, score * 1.1 as total_amount, 1 as order_count " +
                "FROM doris_source " +
                "WHERE age > 20"
        );

        // ==================== Doris主键模型与Flink Upsert ====================

        // 10. 使用Flink Upsert模式更新Doris
        tableEnv.executeSql(
                "CREATE TABLE doris_upsert (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  status STRING," +
                "  update_time TIMESTAMP(3)," +
                "  PRIMARY KEY (order_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:9030/test_db'," +
                "  'table-name' = 'orders'," +
                "  'username' = 'root'," +
                "  'password' = ''," +
                "  'sink.buffer-flush.max-rows' = '1000'," +
                "  'sink.buffer-flush.interval' = '10s'" +
                ")"
        );

        System.out.println("Flink Doris Examples Completed!");
    }
}
