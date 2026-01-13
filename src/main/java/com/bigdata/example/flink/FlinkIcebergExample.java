package com.bigdata.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * Flink + Iceberg Data Lake Examples
 */
public class FlinkIcebergExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ==================== 配置Iceberg Catalog ====================

        // 1. 配置Hadoop Catalog
        tableEnv.executeSql(
                "CREATE CATALOG hadoop_catalog WITH (" +
                "  'type' = 'iceberg'," +
                "  'catalog-type' = 'hadoop'," +
                "  'warehouse' = 'hdfs://namenode:8020/iceberg/warehouse'," +
                "  'property-version' = '1'" +
                ")"
        );

        // 2. 配置Hive Catalog (推荐生产环境使用)
        tableEnv.executeSql(
                "CREATE CATALOG hive_catalog WITH (" +
                "  'type' = 'iceberg'," +
                "  'catalog-type' = 'hive'," +
                "  'uri' = 'thrift://metastore:9083'," +
                "  'clients' = '5'," +
                "  'hive-conf-dir' = '/etc/hive/conf'" +
                ")"
        );

        // 3. 使用默认Catalog
        tableEnv.useCatalog("hadoop_catalog");

        // ==================== 创建Iceberg表 ====================

        // 4. 创建数据库
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS db_analytics");

        // 5. 创建Iceberg表 (Batch写入)
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS db_analytics.users (" +
                "  user_id BIGINT," +
                "  name STRING," +
                "  email STRING," +
                "  age INT," +
                "  created_at TIMESTAMP(3)" +
                ") WITH (" +
                "  'format-version' = '2'," +
                "  'write.format.default' = 'parquet'," +
                "  'write.parquet.compression-codec' = 'gzip'" +
                ")"
        );

        // 6. 创建分区表
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS db_analytics.events (" +
                "  event_id BIGINT," +
                "  event_type STRING," +
                "  user_id BIGINT," +
                "  properties STRING," +
                "  event_time TIMESTAMP(3)" +
                ") PARTITIONED BY (hours(event_time))" +
                "WITH (" +
                "  'format-version' = '2'," +
                "  'write.update.mode' = 'merge-on-read'," +
                "  'write.delete.mode' = 'merge-on-read'" +
                ")"
        );

        // ==================== 写入数据 ====================

        // 7. Batch写入 (批处理)
        // 从DataStream写入
        // env.fromElements(1, 2, 3)
        //     .map(x -> UserData(x, "name" + x))
        //     .sinkTo(...);

        // 8. INSERT INTO (批处理)
        tableEnv.executeSql(
                "INSERT INTO db_analytics.users VALUES " +
                "(1, 'Alice', 'alice@example.com', 25, TIMESTAMP '2024-01-15 10:00:00')," +
                "(2, 'Bob', 'bob@example.com', 30, TIMESTAMP '2024-01-15 11:00:00')"
        );

        // 9. 追加写入
        tableEnv.executeSql(
                "INSERT INTO db_analytics.users " +
                "SELECT user_id, name, email, age, created_at FROM source_users"
        );

        // 10. 覆盖写入
        tableEnv.executeSql(
                "INSERT OVERWRITE db_analytics.users " +
                "SELECT * FROM updated_users"
        );

        // ==================== 实时写入 (Streaming) ====================

        // 11. 创建流式Sink表
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS db_analytics.realtime_orders (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  amount DECIMAL(10, 2)," +
                "  order_time TIMESTAMP(3)," +
                "  proc_time AS PROCTIME()" +
                ") WITH (" +
                "  'connector' = 'iceberg'," +
                "  'catalog-name' = 'hadoop_catalog'," +
                "  'database-name' = 'db_analytics'," +
                "  'table-name' = 'realtime_orders'," +
                "  'write.format.default' = 'parquet'," +
                "  'sink.parallelism' = '4'" +
                ")"
        );

        // 12. 流式写入数据
        // Kafka -> Iceberg
        tableEnv.executeSql(
                "INSERT INTO db_analytics.realtime_orders " +
                "SELECT order_id, user_id, amount, order_time " +
                "FROM kafka_orders"
        );

        // ==================== 查询数据 ====================

        // 13. 基本查询
        Table users = tableEnv.from("db_analytics.users");
        users.select($("user_id"), $("name"), $("age")).execute().print();

        // 14. 聚合查询
        Table aggResult = tableEnv.sqlQuery(
                "SELECT name, COUNT(*) as cnt, AVG(age) as avg_age " +
                "FROM db_analytics.users " +
                "GROUP BY name"
        );

        // 15. 时间旅行查询
        Table timeTravel = tableEnv.sqlQuery(
                "SELECT * FROM db_analytics.users " +
                "TIMESTAMP AS OF '2024-01-15 12:00:00'"
        );

        // 16. 按Snapshot查询
        Table bySnapshot = tableEnv.sqlQuery(
                "SELECT * FROM db_analytics.users " +
                "VERSION AS OF 12345678901234"
        );

        // 17. 查询历史
        Table history = tableEnv.sqlQuery(
                "SELECT * FROM db_analytics.users$history"
        );

        // 18. 查询Snapshots
        Table snapshots = tableEnv.sqlQuery(
                "SELECT * FROM db_analytics.users$snapshots"
        );

        // 19. 查询数据文件
        Table files = tableEnv.sqlQuery(
                "SELECT * FROM db_analytics.users$files"
        );

        // ==================== 维表关联 ====================

        // 20. Iceberg作为维表 (Lookup Join)
        // 先创建Kafka源
        tableEnv.executeSql(
                "CREATE TABLE kafka_orders (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  amount DECIMAL(10, 2)," +
                "  order_time TIMESTAMP(3)," +
                "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'orders'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'format' = 'json'" +
                ")"
        );

        // Lookup Join with Iceberg
        Table enrichedOrders = tableEnv.sqlQuery(
                "SELECT o.order_id, o.user_id, u.name, u.email, o.amount " +
                "FROM kafka_orders o " +
                "JOIN db_analytics.users FOR SYSTEM_TIME AS OF o.proc_time AS u " +
                "ON o.user_id = u.user_id"
        );

        // ==================== 元数据操作 ====================

        // 21. 压缩 (Compaction)
        tableEnv.executeSql(
                "CALL hadoop_catalog.system.rewrite_data_files(" +
                "  table => 'db_analytics.users'," +
                "  options => ('target-file-size-bytes' = '134217728')" +
                ")"
        );

        // 22. 移除过期Snapshot
        tableEnv.executeSql(
                "CALL hadoop_catalog.system.expire_snapshots(" +
                "  table => 'db_analytics.users'," +
                "  older_than => TIMESTAMP '2024-02-01 00:00:00'," +
                "  retain_last => 3" +
                ")"
        );

        // 23. 移除orphan文件
        tableEnv.executeSql(
                "CALL hadoop_catalog.system.remove_orphan_files(" +
                "  table => 'db_analytics.users'," +
                "  older_than => TIMESTAMP '2024-02-01 00:00:00'" +
                ")"
        );

        // 24. 快速Append
        tableEnv.executeSql(
                "CALL hadoop_catalog.system.fast_append(" +
                "  table => 'db_analytics.users'" +
                ")"
        );

        // ==================== 架构演进 ====================

        // 25. 添加列
        tableEnv.executeSql(
                "ALTER TABLE db_analytics.users " +
                "ADD COLUMNS (phone STRING COMMENT 'phone number')"
        );

        // 26. 修改列
        tableEnv.executeSql(
                "ALTER TABLE db_analytics.users " +
                "ALTER COLUMN name TYPE VARCHAR(200)"
        );

        // 27. 重命名列
        tableEnv.executeSql(
                "ALTER TABLE db_analytics.users " +
                "RENAME COLUMN email TO user_email"
        );

        // 28. 删除列
        tableEnv.executeSql(
                "ALTER TABLE db_analytics.users " +
                "DROP COLUMN phone"
        );

        // ==================== 变更数据捕获 (CDC) ====================

        // 29. 使用Flink CDC写入Iceberg
        // 从MySQL CDC写入Iceberg
        tableEnv.executeSql(
                "CREATE TABLE mysql_source (" +
                "  id BIGINT," +
                "  name STRING," +
                "  age INT," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = 'localhost'," +
                "  'port' = '3306'," +
                "  'username' = 'root'," +
                "  'password' = 'password'," +
                "  'database-name' = 'mydb'," +
                "  'table-name' = 'users'" +
                ")"
        );

        tableEnv.executeSql(
                "INSERT INTO db_analytics.users " +
                "SELECT * FROM mysql_source"
        );

        System.out.println("Flink Iceberg Examples Completed!");
    }

    // 内部类
    public static class UserData {
        public long userId;
        public String name;

        public UserData() {}

        public UserData(long userId, String name) {
            this.userId = userId;
            this.name = name;
        }
    }
}
