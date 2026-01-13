package com.bigdata.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.functions;

import java.util.List;

/**
 * Spark + Paimon Data Lake Examples
 * Paimon 是 Apache 流批一体数据湖，提供高效的CDC同步能力
 */
public class SparkPaimonExample {

    private static final String WAREHOUSE_PATH = "hdfs://namenode:8020/paimon/warehouse";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkPaimonExample")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.paimon.spark.PaimonSparkSessionExtensions")
                .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
                .config("spark.sql.catalog.paimon.warehouse", WAREHOUSE_PATH)
                .getOrCreate();

        try {
            System.out.println("Spark Paimon Examples Starting...");

            // 1. 创建Paimon Catalog
            createCatalog(spark);

            // 2. 创建主键表 (Append表)
            createAppendTable(spark);

            // 3. 创建主键表 (Changelog表 - 支持CDC)
            createChangelogTable(spark);

            // 4. 批量写入
            batchWrite(spark);

            // 5. 增量写入
            incrementalWrite(spark);

            // 6. 时间旅行查询
            timeTravelQuery(spark);

            // 7. 变更数据捕获 (CDC) 查询
            cdcQuery(spark);

            // 8. 模式演进
            schemaEvolution(spark);

            // 9. Compaction维护
            compaction(spark);

            System.out.println("Spark Paimon Examples Completed!");

        } finally {
            spark.close();
        }
    }

    /**
     * 创建Catalog
     */
    private static void createCatalog(SparkSession spark) {
        System.out.println("\n=== Create Catalog ===");

        // Paimon Catalog 已通过Spark配置自动创建
        // 可以通过Spark SQL查看Catalog
        spark.sql("SHOW CATALOGS").show();

        // 使用Catalog
        spark.sql("USE paimon");
        spark.sql("SHOW DATABASES").show();
    }

    /**
     * 创建Append表 (适合日志分析，无主键)
     */
    private static void createAppendTable(SparkSession spark) {
        System.out.println("\n=== Create Append Table ===");

        spark.sql(
                "CREATE TABLE IF NOT EXISTS paimon.default.log_events (" +
                "  event_id BIGINT," +
                "  event_type STRING," +
                "  user_id BIGINT," +
                "  event_time TIMESTAMP," +
                "  properties STRING" +
                ") USING paimon " +
                "PARTITIONED BY (event_type) " +
                "TBLPROPERTIES (" +
                "  'bucket' = '4'," +
                "  'write.buffer.size' = '256KB'," +
                "  'compaction.min.file.num' = '3'" +
                ")"
        );

        System.out.println("Append table 'log_events' created successfully");

        // 描述表结构
        spark.sql("DESCRIBE TABLE paimon.default.log_events").show();
    }

    /**
     * 创建Changelog表 (主键表，支持CDC)
     */
    private static void createChangelogTable(SparkSession spark) {
        System.out.println("\n=== Create Changelog Table ===");

        // 创建主键表，支持UPDATE/DELETE操作
        spark.sql(
                "CREATE TABLE IF NOT EXISTS paimon.default.orders (" +
                "  order_id BIGINT," +
                "  user_id BIGINT," +
                "  product_name STRING," +
                "  quantity INT," +
                "  price DECIMAL(10, 2)," +
                "  order_time TIMESTAMP," +
                "  status STRING" +
                ") USING paimon " +
                "PARTITIONED BY (status) " +
                "TBLPROPERTIES (" +
                "  'primary-key' = 'order_id'," +
                "  'bucket' = '8'," +
                "  'merge-engine' = 'deduplicate'," +
                "  'write-mode' = 'changelog'" +
                ")"
        );

        System.out.println("Changelog table 'orders' created successfully");

        // 创建用户维度表
        spark.sql(
                "CREATE TABLE IF NOT EXISTS paimon.default.dim_users (" +
                "  user_id BIGINT," +
                "  user_name STRING," +
                "  email STRING," +
                "  age INT," +
                "  create_time TIMESTAMP" +
                ") USING paimon " +
                "TBLPROPERTIES (" +
                "  'primary-key' = 'user_id'," +
                "  'bucket' = '4'" +
                ")"
        );

        System.out.println("Dim users table created successfully");
    }

    /**
     * 批量写入
     */
    private static void batchWrite(SparkSession spark) {
        System.out.println("\n=== Batch Write ===");

        // 方式1: INSERT OVERWRITE (覆盖写入)
        spark.sql(
                "INSERT OVERWRITE paimon.default.orders VALUES " +
                "(1, 100, 'Product A', 2, 199.98, TIMESTAMP '2024-01-15 10:00:00', 'completed')," +
                "(2, 101, 'Product B', 1, 99.00, TIMESTAMP '2024-01-15 11:00:00', 'completed')," +
                "(3, 102, 'Product C', 3, 299.97, TIMESTAMP '2024-01-15 12:00:00', 'pending')"
        );

        // 方式2: INSERT INTO (追加写入)
        spark.sql(
                "INSERT INTO paimon.default.orders VALUES " +
                "(4, 100, 'Product D', 1, 49.99, TIMESTAMP '2024-01-15 13:00:00', 'processing')"
        );

        // 方式3: 从其他表读取写入
        // spark.sql(
        //     "INSERT INTO paimon.default.orders " +
        //     "SELECT * FROM source_orders WHERE order_date = '2024-01-15'"
        // );

        System.out.println("Batch write completed");
    }

    /**
     * 增量写入 (流式写入)
     */
    private static void incrementalWrite(SparkSession spark) {
        System.out.println("\n=== Incremental Write ===");

        // Paimon支持流式增量写入
        // 注意: 需要使用Spark Structured Streaming

        // 增量写入查询
        spark.sql(
                "CREATE TABLE IF NOT EXISTS paimon.default.order_updates (" +
                "  order_id BIGINT," +
                "  update_type STRING," +
                "  order_time TIMESTAMP" +
                ") USING paimon " +
                "TBLPROPERTIES ('primary-key' = 'order_id')"
        );

        // 写入增量数据
        spark.sql(
                "INSERT INTO paimon.default.order_updates VALUES " +
                "(1, 'update', TIMESTAMP '2024-01-15 14:00:00')," +
                "(5, 'insert', TIMESTAMP '2024-01-15 15:00:00')"
        );

        System.out.println("Incremental write completed");
    }

    /**
     * 时间旅行查询
     */
    private static void timeTravelQuery(SparkSession spark) {
        System.out.println("\n=== Time Travel Query ===");

        // 1. 查询历史版本 (使用版本号)
        System.out.println("Version as of 1:");
        spark.sql("SELECT * FROM paimon.default.orders VERSION AS OF 1").show();

        // 2. 查询历史版本 (使用时间戳)
        System.out.println("Timestamp as of 10 minutes ago:");
        // spark.sql("SELECT * FROM paimon.default.orders TIMESTAMP AS OF '2024-01-15 10:00:00'").show();

        // 3. 查询所有历史版本
        System.out.println("Table history:");
        spark.sql("SELECT * FROM paimon.default.orders$history").show();

        // 4. 查询Snapshots
        System.out.println("Table snapshots:");
        spark.sql("SELECT * FROM paimon.default.orders$snapshots").show();

        // 5. 查询指定Snapshots的数据
        // spark.sql("SELECT * FROM paimon.default.orders SNAPSHOT AS OF 12345678901234").show();
    }

    /**
     * CDC变更数据查询
     */
    private static void cdcQuery(SparkSession spark) {
        System.out.println("\n=== CDC Query ===");

        // 1. 查询所有变更记录
        System.out.println("All changes:");
        spark.sql("SELECT * FROM paimon.default.orders$changes").show();

        // 2. 查询指定主键的变更历史
        System.out.println("Changes for order_id = 1:");
        spark.sql(
                "SELECT * FROM paimon.default.orders$changes " +
                "WHERE order_id = 1"
        ).show();

        // 3. 按时间顺序查看变更
        System.out.println("Changes ordered by commit time:");
        spark.sql(
                "SELECT * FROM paimon.default.orders$changes " +
                "ORDER BY _commit_time"
        ).show();

        // 4. 查询当前最新数据 (主键表)
        System.out.println("Current data:");
        spark.sql("SELECT * FROM paimon.default.orders").show();

        // 5. 查询变更统计
        System.out.println("Change statistics:");
        spark.sql(
                "SELECT _change_type, COUNT(*) as cnt " +
                "FROM paimon.default.orders$changes " +
                "GROUP BY _change_type"
        ).show();
    }

    /**
     * 模式演进
     */
    private static void schemaEvolution(SparkSession spark) {
        System.out.println("\n=== Schema Evolution ===");

        // 1. 添加列
        spark.sql(
                "ALTER TABLE paimon.default.orders " +
                "ADD COLUMNS (shipment_id STRING AFTER status)"
        );
        System.out.println("Added column 'shipment_id'");

        // 2. 修改列
        spark.sql(
                "ALTER TABLE paimon.default.orders " +
                "ALTER COLUMN price TYPE DECIMAL(12, 2)"
        );
        System.out.println("Modified column 'price' type");

        // 3. 重命名列
        spark.sql(
                "ALTER TABLE paimon.default.orders " +
                "RENAME COLUMN shipment_id TO shipping_id"
        );
        System.out.println("Renamed column 'shipment_id' to 'shipping_id'");

        // 4. 查看当前模式
        System.out.println("Current schema:");
        spark.sql("DESCRIBE TABLE paimon.default.orders").show();
    }

    /**
     * Compaction维护
     */
    private static void compaction(SparkSession spark) {
        System.out.println("\n=== Compaction ===");

        // 1. 手动执行Compaction
        spark.sql(
                "CALL paimon.system.compact(" +
                "  table => 'default.orders'," +
                "  options => ('max-concurrent-compaction-jobs' = '4')" +
                ")"
        );
        System.out.println("Compaction completed");

        // 2. 查看文件信息
        System.out.println("Table files:");
        spark.sql("SELECT * FROM paimon.default.orders$files").show();

        // 3. 查看Manifest信息
        System.out.println("Manifest files:");
        spark.sql("SELECT * FROM paimon.default.orders$manifests").show();

        // 4. 移除过期Snapshot
        spark.sql(
                "CALL paimon.system.expire_snapshots(" +
                "  table => 'default.orders'," +
                "  older_than => TIMESTAMP '2024-02-01 00:00:00'," +
                "  retain_last => 10" +
                ")"
        );
        System.out.println("Expired old snapshots");
    }

    /**
     * Join操作
     */
    private static void joinOperations(SparkSession spark) {
        System.out.println("\n=== Join Operations ===");

        // 查询订单与用户信息
        System.out.println("Orders with user details:");
        spark.sql(
                "SELECT o.order_id, o.product_name, u.user_name, u.email " +
                "FROM paimon.default.orders o " +
                "JOIN paimon.default.dim_users u " +
                "ON o.user_id = u.user_id"
        ).show();

        // 多表Join
        System.out.println("Complex join:");
        spark.sql(
                "SELECT o.order_id, o.product_name, u.user_name, e.event_type " +
                "FROM paimon.default.orders o " +
                "JOIN paimon.default.dim_users u ON o.user_id = u.user_id " +
                "LEFT JOIN paimon.default.log_events e " +
                "ON u.user_id = e.user_id " +
                "WHERE o.order_time >= TIMESTAMP '2024-01-15'"
        ).show();
    }

    /**
     * 聚合查询
     */
    private static void aggregationQuery(SparkSession spark) {
        System.out.println("\n=== Aggregation Query ===");

        // 1. 简单聚合
        System.out.println("Order statistics by status:");
        spark.sql(
                "SELECT status, COUNT(*) as order_count, SUM(price) as total_amount " +
                "FROM paimon.default.orders " +
                "GROUP BY status"
        ).show();

        // 2. 按分区聚合
        System.out.println("Daily statistics:");
        spark.sql(
                "SELECT DATE(order_time) as order_date, " +
                "  COUNT(*) as order_count, " +
                "  AVG(quantity) as avg_quantity " +
                "FROM paimon.default.orders " +
                "GROUP BY DATE(order_time)"
        ).show();

        // 3. 窗口函数
        System.out.println("Running total:");
        spark.sql(
                "SELECT order_id, price, " +
                "  SUM(price) OVER (ORDER BY order_time) as running_total " +
                "FROM paimon.default.orders"
        ).show();
    }
}
