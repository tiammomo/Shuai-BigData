package com.bigdata.example.elasticsearch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spark + Elasticsearch Integration Examples
 */
public class SparkElasticsearchExample {

    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;
    private static final String ES_INDEX = "spark_es_index";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkElasticsearchExample")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", 4)
                .getOrCreate();

        try {
            // ==================== 写入ES ====================

            // 1. DataFrame写入ES
            writeDataFrameToES(spark);

            // 2. RDD写入ES
            writeRDDToES(spark);

            // 3. 带ID写入
            writeWithId(spark);

            // ==================== 读取ES ====================

            // 4. 从ES读取到DataFrame
            readFromES(spark);

            // 5. 带过滤读取
            readWithFilter(spark);

            // 6. 下推查询
            pushDownQuery(spark);

            // ==================== Spark SQL操作 ====================

            // 7. ES表查询
            esTableQuery(spark);

            System.out.println("Spark Elasticsearch Examples Completed!");

        } finally {
            spark.close();
        }
    }

    /**
     * DataFrame写入ES
     */
    private static void writeDataFrameToES(SparkSession spark) {
        // 创建测试数据
        List<ESDocument> documents = Arrays.asList(
                new ESDocument("1", "Spark Guide", "Learn Spark", 100.0),
                new ESDocument("2", "Elasticsearch Guide", "Learn ES", 99.0),
                new ESDocument("3", "Flink Guide", "Learn Flink", 88.0)
        );

        Dataset<Row> df = spark.createDataFrame(documents, ESDocument.class);

        // 写入ES
        df.write()
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", ES_HOST)
                .option("es.port", String.valueOf(ES_PORT))
                .option("es.index.auto.create", "true")
                .option("es.mapping.id", "id")
                .mode(SaveMode.Append)
                .save(ES_INDEX);

        System.out.println("DataFrame written to ES: " + ES_INDEX);
    }

    /**
     * RDD写入ES
     */
    private static void writeRDDToES(SparkSession spark) {
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // 创建RDD
        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList(
                "{\"id\":\"10\",\"title\":\"Doc1\",\"content\":\"Content1\",\"price\":50.0}",
                "{\"id\":\"11\",\"title\":\"Doc2\",\"content\":\"Content2\",\"price\":60.0}"
        ));

        // 写入ES
        Map<String, String> esOptions = new HashMap<>();
        esOptions.put("es.nodes", ES_HOST);
        esOptions.put("es.port", String.valueOf(ES_PORT));
        esOptions.put("es.index.auto.create", "true");

        org.apache.spark.api.java.JavaEsSpark.saveJsonToEs(rdd, ES_INDEX, esOptions);

        System.out.println("RDD written to ES: " + ES_INDEX);
    }

    /**
     * 带ID写入 (更新操作)
     */
    private static void writeWithId(SparkSession spark) {
        List<ESDocument> documents = Arrays.asList(
                new ESDocument("1", "Updated Spark Guide", "Updated content", 150.0)
        );

        Dataset<Row> df = spark.createDataFrame(documents, ESDocument.class);

        // 使用upsert模式
        df.write()
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", ES_HOST)
                .option("es.port", String.valueOf(ES_PORT))
                .option("es.write.operation", "upsert")  // 更新或插入
                .option("es.mapping.id", "id")
                .mode(SaveMode.Overwrite)
                .save(ES_INDEX);

        System.out.println("Upsert written to ES");
    }

    /**
     * 从ES读取到DataFrame
     */
    private static void readFromES(SparkSession spark) {
        // 方式1: 使用Spark SQL读取ES
        Dataset<Row> df = spark.read()
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", ES_HOST)
                .option("es.port", String.valueOf(ES_PORT))
                .option("es.query", "?df")  // 简单查询
                .load(ES_INDEX);

        System.out.println("Read from ES, count: " + df.count());
        df.show();

        // 2. 指定读取字段
        Dataset<Row> selectedDf = df.select("id", "title", "price");
        selectedDf.show();
    }

    /**
     * 带过滤读取
     */
    private static void readWithFilter(SparkSession spark) {
        Dataset<Row> df = spark.read()
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", ES_HOST)
                .option("es.port", String.valueOf(ES_PORT))
                .option("es.query", "{\"term\": {\"title.keyword\": \"Spark Guide\"}}")
                .load(ES_INDEX);

        System.out.println("Filtered read results:");
        df.show();
    }

    /**
     * 下推查询优化
     */
    private static void pushDownQuery(SparkSession spark) {
        // 方式1: 使用pushdown predicate
        Dataset<Row> df = spark.read()
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", ES_HOST)
                .option("es.port", String.valueOf(ES_PORT))
                .option("es.read.field.as.array.include", "tags")
                .load(ES_INDEX);

        // 在Spark层进行过滤 (会下推到ES)
        Dataset<Row> filtered = df.filter(df.col("price").gt(50));
        System.out.println("Pushdown query, count: " + filtered.count());
        filtered.show();

        // 方式2: 使用ES query参数
        String esQuery = "{\"range\": {\"price\": {\"gte\": 80}}}";
        Dataset<Row> rangeDf = spark.read()
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", ES_HOST)
                .option("es.port", String.valueOf(ES_PORT))
                .option("es.query", esQuery)
                .load(ES_INDEX);

        System.out.println("Range query results:");
        rangeDf.show();
    }

    /**
     * Spark SQL ES表查询
     */
    private static void esTableQuery(SparkSession spark) {
        // 注册ES表
        spark.read()
                .format("org.elasticsearch.spark.sql")
                .option("es.nodes", ES_HOST)
                .option("es.port", String.valueOf(ES_PORT))
                .load(ES_INDEX)
                .createOrReplaceTempView("es_documents");

        // SQL查询
        Dataset<Row> result = spark.sql(
                "SELECT id, title, price " +
                "FROM es_documents " +
                "WHERE price > 50 " +
                "ORDER BY price DESC " +
                "LIMIT 10"
        );

        System.out.println("SQL Query Results:");
        result.show();

        // 聚合查询
        Dataset<Row> aggResult = spark.sql(
                "SELECT title, COUNT(*) as cnt, AVG(price) as avg_price " +
                "FROM es_documents " +
                "GROUP BY title " +
                "HAVING COUNT(*) > 0"
        );

        System.out.println("Aggregation Results:");
        aggResult.show();

        // LIKE查询
        Dataset<Row> likeResult = spark.sql(
                "SELECT * FROM es_documents " +
                "WHERE title LIKE '%Guide%'"
        );

        System.out.println("LIKE Query Results:");
        likeResult.show();
    }

    /**
     * ES文档POJO
     */
    public static class ESDocument implements java.io.Serializable {
        public String id;
        public String title;
        public String content;
        public Double price;

        public ESDocument() {}

        public ESDocument(String id, String title, String content, Double price) {
            this.id = id;
            this.title = title;
            this.content = content;
            this.price = price;
        }
    }
}
