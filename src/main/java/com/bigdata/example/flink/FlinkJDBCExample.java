package com.bigdata.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * Flink JDBC Connector Examples - MySQL/Doris/PostgreSQL
 */
public class FlinkJDBCExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== MySQL Source ====================

        // 1. 创建MySQL Source表 (CDC同步)
        tableEnv.executeSql(
                "CREATE TABLE mysql_source (" +
                "  id BIGINT," +
                "  name STRING," +
                "  age INT," +
                "  create_time TIMESTAMP(3)," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:3306/mydb'," +
                "  'table-name' = 'users'," +
                "  'username' = 'root'," +
                "  'password' = 'password'" +
                ")"
        );

        // 2. 创建MySQL Sink表 (Append)
        tableEnv.executeSql(
                "CREATE TABLE mysql_sink (" +
                "  user_id BIGINT," +
                "  user_name STRING," +
                "  order_count BIGINT," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:3306/mydb'," +
                "  'table-name' = 'user_stats'," +
                "  'username' = 'root'," +
                "  'password' = 'password'" +
                ")"
        );

        // 3. 创建Doris Source表
        tableEnv.executeSql(
                "CREATE TABLE doris_source (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  amount DOUBLE," +
                "  order_time TIMESTAMP(3)" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://doris-fe:9030/mydb'," +
                "  'table-name' = 'orders'," +
                "  'username' = 'root'," +
                "  'password' = 'password'" +
                ")"
        );

        // 4. 创建Doris Sink表
        tableEnv.executeSql(
                "CREATE TABLE doris_sink (" +
                "  user_id BIGINT," +
                "  total_amount DOUBLE," +
                "  avg_amount DOUBLE," +
                "  order_count BIGINT," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://doris-fe:9030/mydb'," +
                "  'table-name' = 'user_order_stats'," +
                "  'username' = 'root'," +
                "  'password' = 'password'" +
                ")"
        );

        // 5. 创建PostgreSQL Source表
        tableEnv.executeSql(
                "CREATE TABLE pg_source (" +
                "  product_id BIGINT," +
                "  product_name STRING," +
                "  price DECIMAL(10, 2)," +
                "  stock INT" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:postgresql://localhost:5432/mydb'," +
                "  'table-name' = 'products'," +
                "  'username' = 'postgres'," +
                "  'password' = 'password'" +
                ")"
        );

        // ==================== 业务操作 ====================

        // 1. 批量读取数据
        Table batchResult = tableEnv.sqlQuery(
                "SELECT user_id, COUNT(*) as order_count, SUM(amount) as total_amount " +
                "FROM doris_source " +
                "WHERE order_time >= '2024-01-01' " +
                "GROUP BY user_id"
        );

        // 2. 实时写入数据
        batchResult.executeInsert("doris_sink");

        // 3. 维表查询 (Lookup Join)
        // 创建用户维度表
        tableEnv.executeSql(
                "CREATE TABLE dim_user (" +
                "  user_id BIGINT," +
                "  user_name STRING," +
                "  gender STRING," +
                "  age INT," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:3306/mydb'," +
                "  'table-name' = 'dim_user'," +
                "  'username' = 'root'," +
                "  'password' = 'password'" +
                ")"
        );

        // 维表关联查询
        Table ordersWithDim = tableEnv.sqlQuery(
                "SELECT o.order_id, o.user_id, d.user_name, d.age, o.amount " +
                "FROM doris_source o " +
                "JOIN dim_user FOR SYSTEM_TIME AS OF o.proc_time AS d " +
                "ON o.user_id = d.user_id"
        );

        // 4. 双流Join (Temporal Table Join)
        // 注册时态表
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE rates_history (" +
                "  currency STRING," +
                "  rate DECIMAL(10, 4)," +
                "  proc_time AS PROCTIME()" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:3306/mydb'," +
                "  'table-name' = 'exchange_rates'," +
                "  'username' = 'root'," +
                "  'password' = 'password'" +
                ")"
        );

        // 时态表Join
        Table convertedOrders = tableEnv.sqlQuery(
                "SELECT o.order_id, o.amount, o.currency, " +
                "       o.amount * r.rate as amount_cny " +
                "FROM doris_source o, rates_history r " +
                "WHERE o.currency = r.currency"
        );

        // 5. 维表缓存配置
        // 可以通过设置 'lookup.cache.max-rows' 和 'lookup.cache.ttl' 来优化

        // 6. 批量写入配置
        // 'sink.buffer-flush.max-rows' = 1000
        // 'sink.buffer-flush.interval' = 1m
        // 'sink.max-retries' = 3

        System.out.println("Flink JDBC Examples Completed!");
    }
}
