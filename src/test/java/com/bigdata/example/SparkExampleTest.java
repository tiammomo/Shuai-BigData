package com.bigdata.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Spark 示例测试类
 */
public class SparkExampleTest {

    private SparkSession spark;
    private JavaSparkContext sc;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .appName("Test")
                .master("local[*]")
                .getOrCreate();

        sc = new JavaSparkContext(spark.sparkContext());
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    /**
     * 测试 RDD 创建和基本操作
     */
    @Test
    public void testRDDOperations() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // 测试 map 操作
        JavaRDD<Integer> mapped = rdd.map(x -> x * 2);
        List<Integer> result = mapped.collect();

        assertEquals(Arrays.asList(2, 4, 6, 8, 10), result);

        // 测试 filter 操作
        JavaRDD<Integer> filtered = rdd.filter(x -> x > 2);
        List<Integer> filteredResult = filtered.collect();

        assertEquals(Arrays.asList(3, 4, 5), filteredResult);
    }

    /**
     * 测试 reduce 操作
     */
    @Test
    public void testReduceOperation() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data);

        Integer sum = rdd.reduce((a, b) -> a + b);

        assertEquals(Integer.valueOf(15), sum);
    }

    /**
     * 测试 Dataset 操作
     */
    @Test
    public void testDatasetOperations() {
        List<User> users = Arrays.asList(
                new User(1L, "John", 25),
                new User(2L, "Jane", 30),
                new User(3L, "Bob", 25)
        );

        Dataset<User> df = spark.createDataset(users, Encoders.bean(User.class));

        // 测试 filter
        Dataset<User> adults = df.filter("age >= 18");
        assertEquals(3, adults.count());

        // 测试聚合
        Dataset<Integer> ages = df.select("age");
        assertEquals(3, ages.count());
    }

    /**
     * 测试 WordCount
     */
    @Test
    public void testWordCount() {
        List<String> lines = Arrays.asList(
                "hello world",
                "hello flink",
                "hello spark"
        );

        JavaRDD<String> rdd = sc.parallelize(lines);

        JavaRDD<String> words = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaRDD<String> wordOnes = words.mapToPair(word -> new org.apache.spark.api.java.Pair<>(word, 1L));

        JavaRDD<String> wordCounts = wordOnes.reduceByKey((a, b) -> a + b)
                .map(pair -> pair._1 + ": " + pair._2);

        List<String> result = wordCounts.collect();
        assertFalse(result.isEmpty());
        assertTrue(result.contains("hello: 3"));
    }

    /**
     * 用户实体类
     */
    public static class User {
        private Long id;
        private String name;
        private Integer age;

        public User() {}

        public User(Long id, String name, Integer age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public Integer getAge() { return age; }
        public void setAge(Integer age) { this.age = age; }
    }
}
