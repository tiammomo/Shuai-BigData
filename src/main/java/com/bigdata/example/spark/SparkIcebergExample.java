package com.bigdata.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.functions;

import java.util.List;

/**
 * Spark + Iceberg Data Lake Examples
 */
public class SparkIcebergExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkIcebergExample")
                .master("local[*]")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", "hdfs://namenode:8020/iceberg/warehouse")
                .getOrCreate();

        try {
            // ==================== 创建Iceberg表 ====================

            // 1. 创建命名空间(数据库)
            spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.analytics");

            // 2. 创建Iceberg表 (Copy on Write)
            spark.sql(
                    "CREATE TABLE IF NOT EXISTS spark_catalog.analytics.users (" +
                    "  user_id BIGINT, " +
                    "  name STRING, " +
                    "  email STRING, " +
                    "  age INT, " +
                    "  created_at TIMESTAMP" +
                    ") USING iceberg " +
                    "PARTITIONED BY (years(created_at), bucket(16, user_id)) " + // 分区
                    "TBLPROPERTIES (" +
                    "  'write.target-file-size-bytes' = '134217728'," + // 128MB
                    "  'write.format.default' = 'parquet'" +
                    ")"
            );

            // 3. 创建Append-Only表 (适合日志场景)
            spark.sql(
                    "CREATE TABLE IF NOT EXISTS spark_catalog.analytics.events (" +
                    "  event_id BIGINT, " +
                    "  event_type STRING, " +
                    "  user_id BIGINT, " +
                    "  properties STRING, " +
                    "  event_time TIMESTAMP" +
                    ") USING iceberg " +
                    "PARTITIONED BY (hours(event_time))" +
                    "TBLPROPERTIES (" +
                    "  'write.update.mode' = 'merge-on-read'," + // Merge-on-Read
                    "  'write.delete.mode' = 'merge-on-read'" +
                    ")"
            );

            // ==================== 插入数据 ====================

            // 4. 插入单条数据
            spark.sql(
                    "INSERT INTO spark_catalog.analytics.users VALUES " +
                    "(1, 'Alice', 'alice@example.com', 25, TIMESTAMP '2024-01-15 10:30:00')"
            );

            // 5. 批量插入
            spark.sql(
                    "INSERT INTO spark_catalog.analytics.users VALUES " +
                    "(2, 'Bob', 'bob@example.com', 30, TIMESTAMP '2024-01-15 11:00:00')," +
                    "(3, 'Charlie', 'charlie@example.com', 35, TIMESTAMP '2024-01-15 12:00:00')"
            );

            // 6. 从查询插入
            spark.sql(
                    "INSERT INTO spark_catalog.analytics.users " +
                    "SELECT * FROM source_table WHERE status = 'active'"
            );

            // 7. 覆盖写入 (Overwrite)
            spark.sql(
                    "INSERT OVERWRITE spark_catalog.analytics.users " +
                    "SELECT * FROM new_users WHERE active = true"
            );

            // 8. 动态Overwrite (按分区)
            spark.sql(
                    "INSERT OVERWRITE spark_catalog.analytics.users " +
                    "PARTITION (created_at) " +
                    "SELECT user_id, name, email, age, created_at FROM updated_users"
            );

            // ==================== 查询数据 ====================

            // 9. 基本查询
            Dataset<Row> users = spark.sql(
                    "SELECT * FROM spark_catalog.analytics.users WHERE age > 25"
            );
            users.show();

            // 10. 时间旅行查询 (Snapshot读取)
            Dataset<Row> timeTravel = spark.sql(
                    "SELECT * FROM spark_catalog.analytics.users " +
                    "TIMESTAMP AS OF '2024-01-15 12:00:00'"
            );

            // 11. 按Snapshot ID查询
            Dataset<Row> bySnapshot = spark.sql(
                    "SELECT * FROM spark_catalog.analytics.users " +
                    "VERSION AS OF 12345678901234"
            );

            // 12. 查询历史记录
            Dataset<Row> history = spark.sql(
                    "SELECT * FROM spark_catalog.analytics.users$history"
            );

            // 13. 查询Manifest列表
            Dataset<Row> manifests = spark.sql(
                    "SELECT * FROM spark_catalog.analytics.users$manifests"
            );

            // 14. 查询数据文件
            Dataset<Row> dataFiles = spark.sql(
                    "SELECT * FROM spark_catalog.analytics.users$files"
            );

            // ==================== 更新和删除 ====================

            // 15. 更新数据 (Merge-on-Read表)
            spark.sql(
                    "UPDATE spark_catalog.analytics.users " +
                    "SET age = age + 1 " +
                    "WHERE user_id = 1"
            );

            // 16. 删除数据
            spark.sql(
                    "DELETE FROM spark_catalog.analytics.users " +
                    "WHERE user_id = 1"
            );

            // 17. 合并操作 (Merge)
            spark.sql(
                    "MERGE INTO spark_catalog.analytics.users AS target " +
                    "USING updates AS source " +
                    "ON target.user_id = source.user_id " +
                    "WHEN MATCHED THEN UPDATE SET * " +
                    "WHEN NOT MATCHED THEN INSERT *"
            );

            // ==================== 元数据操作 ====================

            // 18. 查看表信息
            Table userTable = spark.sql("DESCRIBE TABLE EXTENDED spark_catalog.analytics.users")
                    .collectAsList().toString();
            System.out.println(userTable);

            // 19. 查看表详情
            spark.sql("DESCRIBE DETAIL spark_catalog.analytics.users").show();

            // 20. 查看表统计信息
            spark.sql("DESCRIBE HISTORY spark_catalog.analytics.users").show();

            // 21. 压缩表 (Compaction)
            spark.sql(
                    "CALL spark_catalog.system.rewrite_data_files(" +
                    "  table => 'spark_catalog.analytics.users')"
            );

            // 22. 移除过期Snapshot
            spark.sql(
                    "CALL spark_catalog.system.expire_snapshots(" +
                    "  table => 'spark_catalog.analytics.users'," +
                    "  older_than => TIMESTAMP '2024-02-01 00:00:00'," +
                    "  retain_last => 3)"
            );

            // ==================== 排序优化 ====================

            // 23. 重新排序
            spark.sql(
                    "CALL spark_catalog.system.rewrite_position_delete_files(" +
                    "  table => 'spark_catalog.analytics.users')"
            );

            // 24. 排序数据文件
            spark.sql(
                    "CALL spark_catalog.system.rewrite_data_files(" +
                    "  table => 'spark_catalog.analytics.users'," +
                    "  strategy => 'sort'," +
                    "  sort_order => 'user_id ASC NULLS LAST, age DESC')"
            );

            // ==================== 架构演进 ====================

            // 25. 添加列
            spark.sql(
                    "ALTER TABLE spark_catalog.analytics.users " +
                    "ADD COLUMNS (phone STRING COMMENT 'phone number')"
            );

            // 26. 修改列
            spark.sql(
                    "ALTER TABLE spark_catalog.analytics.users " +
                    "ALTER COLUMN name TYPE VARCHAR(200)"
            );

            // 27. 重命名列
            spark.sql(
                    "ALTER TABLE spark_catalog.analytics.users " +
                    "RENAME COLUMN email TO user_email"
            );

            // 28. 删除列
            spark.sql(
                    "ALTER TABLE spark_catalog.analytics.users " +
                    "DROP COLUMNS (phone)"
            );

            // ==================== 高级查询 ====================

            // 29. 元数据表查询
            // 查看所有Snapshots
            spark.sql("SELECT * FROM spark_catalog.analytics.users$snapshots").show();

            // 30. 聚合查询
            Dataset<Row> aggResult = spark.sql(
                    "SELECT " +
                    "  event_type, " +
                    "  COUNT(*) as event_count, " +
                    "  COUNT(DISTINCT user_id) as unique_users " +
                    "FROM spark_catalog.analytics.events " +
                    "GROUP BY event_type"
            );

            // 31. 窗口函数
            Dataset<Row> windowResult = spark.sql(
                    "SELECT " +
                    "  user_id, " +
                    "  event_time, " +
                    "  LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) as prev_event " +
                    "FROM spark_catalog.analytics.events"
            );

            // ====================与其他格式集成 ====================

            // 32. 从Parquet读取转Iceberg
            spark.sql(
                    "CREATE TABLE spark_catalog.analytics.orders " +
                    "USING iceberg " +
                    "AS SELECT * FROM parquet.`hdfs://path/to/orders.parquet`"
            );

            // 33. Iceberg转Parquet
            spark.sql(
                    "CREATE TABLE parquet.`hdfs://path/to/output.parquet` " +
                    "USING parquet " +
                    "AS SELECT * FROM spark_catalog.analytics.users"
            );

            System.out.println("Iceberg Examples Completed!");

        } finally {
            spark.close();
        }
    }
}
