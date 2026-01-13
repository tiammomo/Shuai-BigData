package com.bigdata.example.doris;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Spark + Doris Integration Examples
 */
public class DorisSparkExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkDorisExample")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
                .getOrCreate();

        try {
            // ==================== 从Doris读取数据 ====================

            // 1. 方式一: JDBC方式读取
            Dataset<Row> dorisDF = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "test_table")
                    .option("user", "root")
                    .option("password", "")
                    .option("fetchsize", 1000) // 每次fetch行数
                    .load();

            dorisDF.printSchema();
            dorisDF.show();

            // 2. 读取Doris表指定列
            Dataset<Row> dorisDF2 = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "(SELECT id, name, score FROM test_table WHERE score > 60) t")
                    .option("user", "root")
                    .option("password", "")
                    .load();

            // 3. 带谓词下推读取
            Dataset<Row> dorisDF3 = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "test_table")
                    .option("user", "root")
                    .option("password", "")
                    .option("pushDownPredicate", true) // 启用谓词下推
                    .option("pushDownLimit", true)     // 启用Limit下推
                    .load();

            // ==================== 写入数据到Doris ====================

            // 4. 覆盖写入
            Dataset<Row> sourceDF = spark.createDataFrame(
                    java.util.Arrays.asList(
                            new Person(1, "Alice", 25),
                            new Person(2, "Bob", 30),
                            new Person(3, "Charlie", 35)
                    ),
                    Person.class
            );

            sourceDF.write()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "test_table")
                    .option("user", "root")
                    .option("password", "")
                    .mode(SaveMode.Overwrite) // 覆盖写入
                    .save();

            // 5. 追加写入
            sourceDF.write()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "test_table")
                    .option("user", "root")
                    .option("password", "")
                    .mode(SaveMode.Append) // 追加写入
                    .save();

            // 6. 使用批处理配置优化写入
            sourceDF.write()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "test_table")
                    .option("user", "root")
                    .option("password", "")
                    .option("batchsize", 5000)        // 批量大小
                    .option("isolationLevel", "NONE") // 隔离级别
                    .mode(SaveMode.Append)
                    .save();

            // ==================== Doris与Hive集成 ====================

            // 7. Doris作为数据源，Doris表对应Hive外表
            // 需要在Doris中创建Hive外表:
            // CREATE TABLE hive_table (...) ENGINE=OLAP
            // PROPERTIES ("file_cache"="xxx", "dfs_props"="xxx");

            // 8. Spark读写Doris + Hive转换
            Dataset<Row> dorisData = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "test_table")
                    .option("user", "root")
                    .option("password", "")
                    .load();

            // 写入Hive表
            dorisData.write()
                    .mode(SaveMode.Overwrite)
                    .saveAsTable("hive_table");

            // 从Hive表读取，写入Doris
            Dataset<Row> hiveData = spark.read().table("hive_table");
            hiveData.write()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "test_table")
                    .option("user", "root")
                    .option("password", "")
                    .mode(SaveMode.Overwrite)
                    .save();

            // ==================== 复杂查询 ====================

            // 9. Doris聚合查询结果回写
            Dataset<Row> aggregated = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "(SELECT name, COUNT(*) as cnt, AVG(score) as avg_score " +
                            "FROM test_table GROUP BY name) t")
                    .option("user", "root")
                    .option("password", "")
                    .load();

            aggregated.write()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "stats_table")
                    .option("user", "root")
                    .option("password", "")
                    .mode(SaveMode.Overwrite)
                    .save();

            // 10. 双表JOIN后写入
            Dataset<Row> orders = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "orders")
                    .option("user", "root")
                    .option("password", "")
                    .load();

            Dataset<Row> users = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "users")
                    .option("user", "root")
                    .option("password", "")
                    .load();

            // JOIN
            Dataset<Row> joined = orders.join(users, orders.col("user_id").equalTo(users.col("id")));

            // 写回Doris
            joined.write()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:9030/test_db")
                    .option("dbtable", "order_user_detail")
                    .option("user", "root")
                    .option("password", "")
                    .mode(SaveMode.Overwrite)
                    .save();

            System.out.println("Spark Doris Examples Completed!");

        } finally {
            spark.close();
        }
    }

    public static class Person {
        public int id;
        public String name;
        public int age;

        public Person() {}

        public Person(int id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }
    }
}
