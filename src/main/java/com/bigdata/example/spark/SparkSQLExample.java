package com.bigdata.example.spark;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;

/**
 * Spark SQL API Examples - DataFrame和SQL操作示例
 */
public class SparkSQLExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkSQLExample")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "hdfs://path/to/warehouse")
                .getOrCreate();

        try {
            // 1. 创建DataFrame的多种方式

            // 从集合创建
            List<Person> peopleList = Arrays.asList(
                    new Person("Alice", 25, "F"),
                    new Person("Bob", 30, "M"),
                    new Person("Charlie", 35, "M")
            );
            Dataset<Row> df1 = spark.createDataFrame(peopleList, Person.class);

            // 从JSON创建
            Dataset<Row> df2 = spark.read().json("hdfs://path/to/data.json");

            // 从Parquet创建
            Dataset<Row> df3 = spark.read().parquet("hdfs://path/to/data.parquet");

            // 从JDBC创建
            Dataset<Row> df4 = spark.read()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:3306/db")
                    .option("dbtable", "table_name")
                    .option("user", "user")
                    .option("password", "password")
                    .load();

            // 从Hive表创建
            // Dataset<Row> df5 = spark.sql("SELECT * FROM hive_table");

            // 2. DSL风格API操作

            // 选择列
            Dataset<Row> selected = df1.select("name", "age");

            // 添加新列
            Dataset<Row> withColumn = df1.withColumn("age_after_5_years", col("age").plus(5));

            // 重命名列
            Dataset<Row> renamed = df1.withColumnRenamed("name", "person_name");

            // 过滤
            Dataset<Row> filtered = df1.filter(col("age").gt(25));

            // 去重
            Dataset<Row> distinct = df1.distinct();

            // 排序
            Dataset<Row> sorted = df1.orderBy(col("age").desc());

            // 分组聚合
            Dataset<Row> grouped = df1.groupBy("gender").agg(
                    avg("age").alias("avg_age"),
                    count("*").alias("count")
            );

            // 3. SQL风格操作

            // 注册临时视图
            df1.createOrReplaceTempView("person");

            Dataset<Row> sqlResult = spark.sql(
                    "SELECT gender, COUNT(*) as count, AVG(age) as avg_age " +
                    "FROM person " +
                    "WHERE age > 20 " +
                    "GROUP BY gender " +
                    "ORDER BY count DESC"
            );

            // 4. 常用转换操作

            // 限制
            Dataset<Row> limited = df1.limit(10);

            // 采样
            Dataset<Row> sampled = df1.sample(false, 0.5, 123L);

            // 交并差
            Dataset<Row> union = df1.union(df1);
            Dataset<Row> intersect = df1.intersect(df1);
            Dataset<Row> except = df1.except(df1);

            // 5. Join操作

            Dataset<Row> df2 = spark.createDataFrame(
                    Arrays.asList(
                            new Department(1, "Engineering"),
                            new Department(2, "Sales")
                    ),
                    Department.class
            );

            // 内连接
            Dataset<Row> joined = df1.join(df2, col("dept_id").equalTo(col("id")));

            // 左外连接
            Dataset<Row> leftJoined = df1.join(df2, col("dept_id").equalTo(col("id")), "left");

            // 右外连接
            Dataset<Row> rightJoined = df1.join(df2, col("dept_id").equalTo(col("id")), "right");

            // 6. 窗口函数

            df1.createOrReplaceTempView("person");
            Dataset<Row> windowResult = spark.sql(
                    "SELECT name, age, gender, " +
                    "ROW_NUMBER() OVER (PARTITION BY gender ORDER BY age DESC) as rank " +
                    "FROM person"
            );

            // 7. UDF使用

            // 注册UDF
            spark.udf().register("myUpper", (UDF1<String, String>) String::toUpperCase, DataTypes.StringType);

            Dataset<Row> udfResult = spark.sql(
                    "SELECT name, myUpper(name) as upper_name FROM person"
            );

            // 8. 写出数据

            // 写出到JSON
            df1.write().mode(SaveMode.Overwrite).json("hdfs://path/to/output.json");

            // 写出到Parquet
            df1.write().mode(SaveMode.Append).parquet("hdfs://path/to/output.parquet");

            // 写出到JDBC
            df1.write()
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:3306/db")
                    .option("dbtable", "target_table")
                    .option("user", "user")
                    .option("password", "password")
                    .mode(SaveMode.Overwrite)
                    .save();

            // 9. 类型转换和RDD互操作

            // DataFrame -> RDD
            JavaRDD<Row> rdd = df1.toJavaRDD();

            // RDD -> DataFrame
            JavaRDD<Person> personRDD = spark.sparkContext()
                    .parallelize(peopleList)
                    .map(p -> p);
            Dataset<Row> fromRDD = spark.createDataFrame(personRDD, Person.class);

            // DataFrame -> List
            List<Row> rows = df1.collectAsList();

            System.out.println("Spark SQL Examples Completed!");

        } finally {
            spark.close();
        }
    }

    // 内部类
    public static class Person {
        public String name;
        public int age;
        public String gender;

        public Person() {}

        public Person(String name, int age, String gender) {
            this.name = name;
            this.age = age;
            this.gender = gender;
        }
    }

    public static class Department {
        public int id;
        public String name;

        public Department() {}

        public Department(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
