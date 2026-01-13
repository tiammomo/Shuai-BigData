package com.bigdata.example.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark Hive Integration Examples
 */
public class SparkHiveExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkHiveExample")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
                .enableHiveSupport() // 启用Hive支持
                .getOrCreate();

        try {
            // 1. 创建Hive表
            spark.sql(
                    "CREATE TABLE IF NOT EXISTS test_table (" +
                    "  id INT, " +
                    "  name STRING, " +
                    "  age INT, " +
                    "  dt STRING" +
                    ") PARTITIONED BY (dt STRING)" +
                    "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"
            );

            // 2. 插入数据
            spark.sql(
                    "INSERT INTO test_table PARTITION (dt='2024-01-01') " +
                    "VALUES (1, 'Alice', 25), (2, 'Bob', 30)"
            );

            // 3. 查询Hive表
            Dataset<Row> df = spark.sql("SELECT * FROM test_table WHERE dt = '2024-01-01'");

            // 4. DataFrame写入Hive表
            Dataset<Row> dataFrame = spark.createDataFrame(
                    java.util.Arrays.asList(
                            new PersonWithDept("Charlie", 35, "Engineering"),
                            new PersonWithDept("David", 40, "Sales")
                    ),
                    PersonWithDept.class
            );

            dataFrame.createOrReplaceTempView("temp_view");
            spark.sql(
                    "INSERT INTO test_table PARTITION (dt='2024-01-02') " +
                    "SELECT id, name, age, '2024-01-02' FROM temp_view"
            );

            // 5. 动态分区插入
            spark.sql("SET hive.exec.dynamic.partition=true");
            spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict");

            spark.sql(
                    "INSERT INTO TABLE test_table PARTITION (dt) " +
                    "SELECT id, name, age, dt FROM other_table"
            );

            // 6. Hive UDF使用
            spark.sql("SELECT my_udf(name) FROM test_table");

            System.out.println("Hive Examples Completed!");

        } finally {
            spark.close();
        }
    }

    public static class PersonWithDept {
        public String name;
        public int age;
        public String department;

        public PersonWithDept() {}

        public PersonWithDept(String name, int age, String department) {
            this.name = name;
            this.age = age;
            this.department = department;
        }
    }
}
