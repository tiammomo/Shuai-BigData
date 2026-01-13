package com.bigdata.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Spark RDD API Examples - 基础RDD操作示例
 */
public class SparkRDDExample {

    public static void main(String[] args) {
        // 1. 创建SparkConf和JavaSparkContext
        SparkConf conf = new SparkConf()
                .setAppName("SparkRDDExample")
                .setMaster("local[*]"); // 本地模式，使用所有核心

        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // 2. 创建RDD的多种方式
            // 从集合创建
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
            JavaRDD<Integer> rdd1 = sc.parallelize(data);

            // 从文件创建
            JavaRDD<String> rdd2 = sc.textFile("hdfs://path/to/file.txt");

            // 从其他RDD转换
            JavaRDD<Integer> rdd3 = rdd1.map(x -> x * 2);

            // 3. 转换操作(Transformations)
            // map: 对每个元素应用函数
            JavaRDD<Integer> mappedRDD = rdd1.map(x -> x * 2);

            // filter: 过滤元素
            JavaRDD<Integer> filteredRDD = rdd1.filter(x -> x > 2);

            // flatMap: 扁平化映射
            JavaRDD<String> flatMappedRDD = sc.parallelize(Arrays.asList("hello world", "spark"))
                    .flatMap(s -> Arrays.asList(s.split(" ")).iterator());

            // mapPartitions: 对每个分区应用函数(高效)
            JavaRDD<Integer> mapPartitionsRDD = rdd1.mapPartitions(iter -> {
                int sum = 0;
                while (iter.hasNext()) {
                    sum += iter.next();
                }
                return Arrays.asList(sum).iterator();
            });

            // reduceByKey: 按key聚合
            JavaRDD<String> pairRDD = sc.parallelize(Arrays.asList("a", "b", "a", "c", "b", "a"));
            JavaRDD<String> reducedRDD = pairRDD
                    .mapToPair(word -> new scala.Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b)
                    .map(tuple -> tuple._1() + ":" + tuple._2());

            // groupByKey: 按key分组
            JavaRDD<Iterable<Integer>> groupedRDD = pairRDD
                    .mapToPair(word -> new scala.Tuple2<>(word, 1))
                    .groupByKey()
                    .map(tuple -> tuple._2());

            // sortBy: 排序
            JavaRDD<Integer> sortedRDD = rdd1.sortBy(x -> x, true, 1);

            // union: 并集
            JavaRDD<Integer> unionRDD = rdd1.union(sc.parallelize(Arrays.asList(6, 7, 8)));

            // intersection: 交集
            JavaRDD<Integer> intersectionRDD = rdd1.intersection(sc.parallelize(Arrays.asList(3, 4, 5, 6)));

            // distinct: 去重
            JavaRDD<Integer> distinctRDD = sc.parallelize(Arrays.asList(1, 2, 2, 3, 3, 3)).distinct();

            // 4. 动作操作(Actions)
            // collect: 收集所有元素到driver
            List<Integer> collected = rdd1.collect();

            // count: 计数
            long count = rdd1.count();

            // first: 获取第一个元素
            Integer first = rdd1.first();

            // take: 获取前N个元素
            List<Integer> taken = rdd1.take(3);

            // reduce: 聚合
            Integer sum = rdd1.reduce((a, b) -> a + b);

            // countByKey: 按key计数
            Map<String, Long> countByKey = pairRDD
                    .mapToPair(word -> new scala.Tuple2<>(word, 1))
                    .countByKey();

            // foreach: 遍历每个元素
            rdd1.foreach(x -> System.out.println("Element: " + x));

            // takeSample: 采样
            List<Integer> sampled = rdd1.takeSample(false, 3, 123L);

            // 5. 持久化操作
            // cache: 缓存到内存
            rdd1.cache();

            // persist: 指定存储级别
            // rdd1.persist(StorageLevel.MEMORY_AND_DISK());

            // unpersist: 移除缓存
            // rdd1.unpersist();

            System.out.println("RDD Examples Completed!");
            System.out.println("Count: " + count);
            System.out.println("Sum: " + sum);

        } finally {
            sc.close();
        }
    }
}
