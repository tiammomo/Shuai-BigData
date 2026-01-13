package com.bigdata.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * Flink + Paimon Data Lake Examples
 * Paimon 是 Apache 流批一体数据湖，提供高效的CDC同步能力
 */
public class FlinkPaimonExample {

    private static final String WAREHOUSE_PATH = "hdfs://namenode:8020/paimon/warehouse";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        System.out.println("Flink Paimon Examples Starting...");

        // 1. 创建Paimon Catalog
        createCatalog(tableEnv);

        // 2. 创建Append表 (无主键)
        createAppendTable(tableEnv);

        // 3. 创建主键表 (支持CDC)
        createPrimaryKeyTable(tableEnv);

        // 4. 批处理写入
        batchWrite(tableEnv);

        // 5. 流式写入 (CDC同步)
        streamingWrite(tableEnv);

        // 6. 时间旅行查询
        timeTravelQuery(tableEnv);

        // 7. 变更数据查询
        cdcQuery(tableEnv);

        // 8. 维表关联
        lookupJoin(tableEnv);

        // 9. 流式聚合
        streamingAggregation(tableEnv);

        // 10. 模式演进
        schemaEvolution(tableEnv);

        // 11. Compaction维护
        compaction(tableEnv);

        System.out.println("Flink Paimon Examples Completed!");
    }

    /**
     * 创建Paimon Catalog
     */
    private static void createCatalog(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Create Catalog ===");

        // 通过Table API创建Catalog
        tableEnv.executeSql(
                "CREATE CATALOG IF NOT EXISTS paimon_catalog WITH (" +
                "  'type' = 'paimon'," +
                "  'warehouse' = '" + WAREHOUSE_PATH + "'," +
                "  'table.type' = 'paimon'" +
                ")"
        );

        tableEnv.executeSql("USE CATALOG paimon_catalog");

        // 查看当前数据库
        tableEnv.executeSql("SHOW DATABASES").print();
    }

    /**
     * 创建Append表 (无主键，适合日志分析)
     */
    private static void createAppendTable(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Create Append Table ===");

        // 创建Append表 (无主键，不可更新)
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS log_events (" +
                "  event_id BIGINT," +
                "  event_type STRING," +
                "  user_id BIGINT," +
                "  event_time TIMESTAMP(3)," +
                "  message STRING" +
                ") WITH (" +
                "  'connector' = 'paimon'," +
                "  'path' = '" + WAREHOUSE_PATH + "/default/log_events'," +
                "  'bucket' = '4'," +
                "  'write.buffer.size' = '256KB'" +
                ")"
        );

        System.out.println("Append table 'log_events' created");
    }

    /**
     * 创建主键表 (支持CDC)
     */
    private static void createPrimaryKeyTable(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Create Primary Key Table ===");

        // 创建主键表 (Changelog表，支持UPDATE/DELETE)
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS orders (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  product_name STRING," +
                "  quantity INT," +
                "  price DECIMAL(10, 2)," +
                "  order_time TIMESTAMP(3)," +
                "  status STRING," +
                "  PRIMARY KEY (order_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'paimon'," +
                "  'path' = '" + WAREHOUSE_PATH + "/default/orders'," +
                "  'bucket' = '8'," +
                "  'merge-engine' = 'deduplicate'," +
                "  'write-mode' = 'changelog'" +
                ")"
        );

        // 创建用户维度表
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS dim_users (" +
                "  user_id BIGINT," +
                "  user_name STRING," +
                "  email STRING," +
                "  age INT," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'paimon'," +
                "  'path' = '" + WAREHOUSE_PATH + "/default/dim_users'," +
                "  'bucket' = '4'" +
                ")"
        );

        System.out.println("Primary key tables created");
    }

    /**
     * 批处理写入
     */
    private static void batchWrite(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Batch Write ===");

        // 开启批处理模式
        tableEnv.getConfig().set("table.exec.batch-mode", true);

        // 方式1: INSERT OVERWRITE (覆盖写入)
        tableEnv.executeSql(
                "INSERT OVERWRITE orders VALUES " +
                "(1, 100, 'Product A', 2, 199.99, TIMESTAMP '2024-01-15 10:00:00', 'completed')," +
                "(2, 101, 'Product B', 1, 99.00, TIMESTAMP '2024-01-15 11:00:00', 'completed')," +
                "(3, 102, 'Product C', 3, 299.97, TIMESTAMP '2024-01-15 12:00:00', 'pending')"
        );

        // 方式2: INSERT INTO (追加写入)
        tableEnv.executeSql(
                "INSERT INTO orders VALUES " +
                "(4, 100, 'Product D', 1, 49.99, TIMESTAMP '2024-01-15 13:00:00', 'processing')"
        );

        // 方式3: 插入用户数据
        tableEnv.executeSql(
                "INSERT INTO dim_users VALUES " +
                "(100, 'Alice', 'alice@example.com', 25)," +
                "(101, 'Bob', 'bob@example.com', 30)," +
                "(102, 'Charlie', 'charlie@example.com', 35)"
        );

        System.out.println("Batch write completed");
    }

    /**
     * 流式写入 (CDC同步)
     */
    private static void streamingWrite(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Streaming Write ===");

        // 关闭批处理模式
        tableEnv.getConfig().set("table.exec.batch-mode", false);

        // 创建Kafka Source (模拟CDC数据)
        tableEnv.executeSql(
                "CREATE TABLE kafka_orders (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  product_name STRING," +
                "  quantity INT," +
                "  price DECIMAL(10, 2)," +
                "  order_time TIMESTAMP(3)," +
                "  status STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'orders-topic'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'paimon-consumer'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")"
        );

        // 实时同步Kafka数据到Paimon
        tableEnv.executeSql(
                "INSERT INTO orders SELECT * FROM kafka_orders"
        );

        System.out.println("Streaming write started");

        // MySQL CDC同步示例
        // tableEnv.executeSql(
        //     "CREATE TABLE mysql_orders (" +
        //     "  order_id BIGINT," +
        //     "  user_id BIGINT," +
        //     "  product_name STRING," +
        //     "  quantity INT," +
        //     "  price DECIMAL(10, 2)," +
        //     "  order_time TIMESTAMP(3)," +
        //     "  status STRING," +
        //     "  PRIMARY KEY (order_id) NOT ENFORCED" +
        //     ") WITH (" +
        //     "  'connector' = 'mysql-cdc'," +
        //     "  'hostname' = 'localhost'," +
        //     "  'port' = '3306'," +
        //     "  'username' = 'root'," +
        //     "  'password' = 'password'," +
        //     "  'database-name' = 'mydb'," +
        //     "  'table-name' = 'orders'" +
        //     ")"
        // );

        // tableEnv.executeSql(
        //     "INSERT INTO orders SELECT * FROM mysql_orders"
        // );
    }

    /**
     * 时间旅行查询
     */
    private static void timeTravelQuery(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Time Travel Query ===");

        // 开启批处理
        tableEnv.getConfig().set("table.exec.batch-mode", true);

        // 查询指定Snapshot
        System.out.println("Query snapshot 1:");
        Table result1 = tableEnv.sqlQuery(
                "SELECT * FROM orders /*+ OPTIONS('scan.snapshot-id' = '1') */"
        );
        result1.execute().print();

        // 查询最新Snapshot
        System.out.println("Latest data:");
        Table latest = tableEnv.sqlQuery("SELECT * FROM orders");
        latest.execute().print();

        // 查询历史记录
        System.out.println("Table history:");
        Table history = tableEnv.sqlQuery("SELECT * FROM orders$history");
        history.execute().print();

        // 查询Snapshots
        System.out.println("Table snapshots:");
        Table snapshots = tableEnv.sqlQuery("SELECT * FROM orders$snapshots");
        snapshots.execute().print();
    }

    /**
     * CDC变更数据查询
     */
    private static void cdcQuery(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== CDC Query ===");

        // 查询所有变更记录
        System.out.println("All changes:");
        Table changes = tableEnv.sqlQuery(
                "SELECT * FROM orders$changes"
        );
        changes.execute().print();

        // 查询指定主键的变更历史
        System.out.println("Changes for order_id = 1:");
        Table orderChanges = tableEnv.sqlQuery(
                "SELECT * FROM orders$changes WHERE order_id = 1"
        );
        orderChanges.execute().print();

        // 查询变更类型统计
        System.out.println("Change type statistics:");
        Table stats = tableEnv.sqlQuery(
                "SELECT _change_type, COUNT(*) as cnt " +
                "FROM orders$changes " +
                "GROUP BY _change_type"
        );
        stats.execute().print();
    }

    /**
     * 维表关联 (Lookup Join)
     */
    private static void lookupJoin(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Lookup Join ===");

        // 关闭批处理模式
        tableEnv.getConfig().set("table.exec.batch-mode", false);

        // 实时订单流
        tableEnv.executeSql(
                "CREATE TABLE realtime_orders (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  product_name STRING," +
                "  quantity INT," +
                "  order_time TIMESTAMP(3)," +
                "  proc_time AS PROCTIME()" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'realtime-orders'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'format' = 'json'" +
                ")"
        );

        // Lookup Join with Paimon维表
        Table enrichedOrders = tableEnv.sqlQuery(
                "SELECT o.order_id, o.product_name, u.user_name, u.email, " +
                "       o.quantity * o.price as total_amount " +
                "FROM realtime_orders o " +
                "JOIN dim_users FOR SYSTEM_TIME AS OF o.proc_time AS u " +
                "ON o.user_id = u.user_id"
        );

        enrichedOrders.execute().print();
    }

    /**
     * 流式聚合
     */
    private static void streamingAggregation(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Streaming Aggregation ===");

        tableEnv.getConfig().set("table.exec.batch-mode", false);

        // 实时统计每分钟订单数
        Table orderStats = tableEnv.sqlQuery(
                "SELECT " +
                "  TUMBLE_START(order_time, INTERVAL '1' MINUTE) as window_start, " +
                "  COUNT(*) as order_count, " +
                "  SUM(price) as total_amount, " +
                "  COUNT(DISTINCT user_id) as unique_users " +
                "FROM orders " +
                "GROUP BY TUMBLE(order_time, INTERVAL '1' MINUTE)"
        );

        // 写出到另一个Paimon表
        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS order_stats (" +
                "  window_start TIMESTAMP(3)," +
                "  order_count BIGINT," +
                "  total_amount DECIMAL(15, 2)," +
                "  unique_users BIGINT" +
                ") WITH (" +
                "  'connector' = 'paimon'," +
                "  'path' = '" + WAREHOUSE_PATH + "/default/order_stats'," +
                "  'bucket' = '1'" +
                ")"
        );

        orderStats.executeInsert("orderStats");
        System.out.println("Streaming aggregation started");
    }

    /**
     * 模式演进
     */
    private static void schemaEvolution(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Schema Evolution ===");

        // 1. 添加列
        tableEnv.executeSql(
                "ALTER TABLE orders ADD COLUMN shipment_id STRING AFTER status"
        );
        System.out.println("Added column 'shipment_id'");

        // 2. 修改列
        tableEnv.executeSql(
                "ALTER TABLE orders MODIFY COLUMN price DECIMAL(12, 2)"
        );
        System.out.println("Modified column 'price'");

        // 3. 重命名列
        tableEnv.executeSql(
                "ALTER TABLE orders RENAME COLUMN shipment_id TO shipping_id"
        );
        System.out.println("Renamed column 'shipment_id' to 'shipping_id'");

        // 4. 查看当前模式
        System.out.println("Current schema:");
        tableEnv.executeSql("DESCRIBE orders").print();
    }

    /**
     * Compaction维护
     */
    private static void compaction(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Compaction ===");

        // 1. 手动执行Compaction
        tableEnv.executeSql(
                "CALL paimon.system.compact(" +
                "  table => 'default.orders'," +
                "  options => ('max-concurrent-compaction-jobs' = '4')" +
                ")"
        );
        System.out.println("Compaction completed");

        // 2. 查看文件信息
        System.out.println("Table files:");
        tableEnv.executeSql("SELECT * FROM orders$files").print();

        // 3. 移除过期Snapshot (保留最近7天和最后3个)
        tableEnv.executeSql(
                "CALL paimon.system.expire_snapshots(" +
                "  table => 'default.orders'," +
                "  older_than => TIMESTAMP '2024-02-01 00:00:00'," +
                "  retain_last => 3" +
                ")"
        );
        System.out.println("Expired old snapshots");

        // 4. 清理orphan文件
        tableEnv.executeSql(
                "CALL paimon.system.delete_orphan_files(" +
                "  table => 'default.orders'," +
                "  older_than => TIMESTAMP '2024-02-01 00:00:00'" +
                ")"
        );
        System.out.println("Cleaned orphan files");
    }

    /**
     * 创建Paimon Sink (用于Flink SQL)
     */
    private static void createPaimonSink(StreamTableEnvironment tableEnv) {
        System.out.println("\n=== Paimon Sink ===");

        // 创建Paimon Sink表
        tableEnv.executeSql(
                "CREATE TABLE paimon_sink (" +
                "  id BIGINT," +
                "  data STRING," +
                "  PRIMARY KEY (id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'paimon'," +
                "  'path' = '" + WAREHOUSE_PATH + "/default/paimon_sink'," +
                "  'sink.parallelism' = '4'" +
                ")"
        );

        // 从其他表写入
        // tableEnv.executeSql(
        //     "INSERT INTO paimon_sink SELECT id, data FROM source_table"
        // );
    }
}
