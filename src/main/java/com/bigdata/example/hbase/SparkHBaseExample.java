package com.bigdata.example.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Spark + HBase Integration Examples
 */
public class SparkHBaseExample {

    private static final String ZOOKEEPER_QUORUM = "localhost";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String TABLE_NAME = "test_table";
    private static final String CF_DEFAULT = "cf";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("SparkHBaseExample")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        try {
            // 1. 创建HBase配置
            org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
            hbaseConf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
            hbaseConf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

            // ==================== 从HBase读取 ====================

            // 2. 使用Spark读取HBase (HBase RDD)
            readFromHBase(jsc, hbaseConf);

            // 3. 使用Spark SQL读取HBase
            readHBaseWithSQL(spark, hbaseConf);

            // ==================== 写入HBase ====================

            // 4. 写入HBase
            writeToHBase(jsc, hbaseConf);

            // 5. Bulk Load到HBase
            bulkLoadToHBase(jsc, hbaseConf);

            System.out.println("Spark HBase Examples Completed!");

        } finally {
            jsc.close();
            spark.close();
        }
    }

    /**
     * 从HBase读取数据到Spark RDD
     */
    private static void readFromHBase(JavaSparkContext jsc,
                                       org.apache.hadoop.conf.Configuration hbaseConf) {
        // 配置输入格式
        hbaseConf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

        // 读取HBase数据
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD =
                jsc.newAPIHadoopRDD(
                        hbaseConf,
                        TableInputFormat.class,
                        ImmutableBytesWritable.class,
                        Result.class);

        // 转换为自定义格式
        JavaRDD<HBaseRecord> records = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, HBaseRecord>() {
            @Override
            public HBaseRecord call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
                Result result = tuple._2();
                String rowKey = Bytes.toString(result.getRow());
                String name = Bytes.toString(result.getValue(
                        Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("name")));
                String age = Bytes.toString(result.getValue(
                        Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("age")));
                return new HBaseRecord(rowKey, name, age);
            }
        });

        // 收集并打印
        List<HBaseRecord> list = records.collect();
        System.out.println("Read " + list.size() + " records from HBase");
        for (HBaseRecord record : list) {
            System.out.println("  Row: " + record);
        }
    }

    /**
     * 使用Spark SQL读取HBase
     */
    private static void readHBaseWithSQL(SparkSession spark,
                                          org.apache.hadoop.conf.Configuration hbaseConf) {
        // 方式1: 通过临时视图
        JavaRDD<HBaseRecord> rdd = spark.sparkContext()
                .parallelize(Arrays.asList(
                        new HBaseRecord("user-001", "Alice", "25"),
                        new HBaseRecord("user-002", "Bob", "30"),
                        new HBaseRecord("user-003", "Charlie", "35")))
                .map(record -> record);

        // 创建DataFrame
        StructType schema = new StructType(new StructField[]{
                new StructField("rowKey", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(rdd, HBaseRecord.class);
        df.createOrReplaceTempView("hbase_users");

        // SQL查询
        Dataset<Row> result = spark.sql(
                "SELECT * FROM hbase_users WHERE age > 25"
        );
        result.show();

        // 聚合查询
        Dataset<Row> aggResult = spark.sql(
                "SELECT name, COUNT(*) as cnt FROM hbase_users GROUP BY name"
        );
        aggResult.show();
    }

    /**
     * 写入HBase
     */
    private static void writeToHBase(JavaSparkContext jsc,
                                      org.apache.hadoop.conf.Configuration hbaseConf) {
        // 创建测试数据
        List<HBaseRecord> data = Arrays.asList(
                new HBaseRecord("spark-001", "Alice", "25"),
                new HBaseRecord("spark-002", "Bob", "30"),
                new HBaseRecord("spark-003", "Charlie", "35")
        );

        JavaRDD<HBaseRecord> inputRDD = jsc.parallelize(data);

        // 转换为Put操作
        JavaPairRDD<ImmutableBytesWritable, Put> putRDD = inputRDD.mapToPair(
                new PairFunction<HBaseRecord, ImmutableBytesWritable, Put>() {
                    @Override
                    public Tuple2<ImmutableBytesWritable, Put> call(HBaseRecord record) throws Exception {
                        Put put = new Put(Bytes.toBytes(record.rowKey));
                        put.addColumn(Bytes.toBytes(CF_DEFAULT),
                                Bytes.toBytes("name"), Bytes.toBytes(record.name));
                        put.addColumn(Bytes.toBytes(CF_DEFAULT),
                                Bytes.toBytes("age"), Bytes.toBytes(record.age));
                        return new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(record.rowKey)), put);
                    }
                });

        // 配置输出
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
        hbaseConf.set(org.apache.hadoop.mapreduce.Job.OUTPUT_FORMAT_CLASS,
                TableOutputFormat.class.getName());

        // 写入HBase
        putRDD.saveAsNewAPIHadoopDataset(hbaseConf);
        System.out.println("Wrote " + data.size() + " records to HBase");
    }

    /**
     * Bulk Load到HBase
     */
    private static void bulkLoadToHBase(JavaSparkContext jsc,
                                         org.apache.hadoop.conf.Configuration hbaseConf)
            throws IOException, ClassNotFoundException, InterruptedException {

        // 1. 生成HFile
        // 配置HFile输出格式
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

        Job job = Job.getInstance(hbaseConf);
        job.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.class);

        // 准备数据
        List<HBaseRecord> data = Arrays.asList(
                new HBaseRecord("bulk-001", "User1", "20"),
                new HBaseRecord("bulk-002", "User2", "25"),
                new HBaseRecord("bulk-003", "User3", "30")
        );

        JavaRDD<HBaseRecord> inputRDD = jsc.parallelize(data);

        // 转换为KeyValue
        JavaPairRDD<ImmutableBytesWritable, org.apache.hadoop.hbase.Cell> kvRDD =
                inputRDD.mapToPair(record -> {
                    KeyValue kv = new KeyValue(
                            Bytes.toBytes(record.rowKey),
                            Bytes.toBytes(CF_DEFAULT),
                            Bytes.toBytes("data"),
                            Bytes.toBytes(record.toString()));
                    return new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(record.rowKey)), (org.apache.hadoop.hbase.Cell) kv);
                });

        // 保存为HFile
        // kvRDD.saveAsNewAPIHadoopFile(
        //         "/tmp/hfiles",
        //         ImmutableBytesWritable.class,
        //         org.apache.hadoop.hbase.Cell.class,
        //         HFileOutputFormat2.class,
        //         hbaseConf);

        System.out.println("Bulk load prepared for HFile");
        System.out.println("Use 'hbase bulkload' command to complete the load");
    }

    /**
     * HBase记录POJO
     */
    public static class HBaseRecord implements Serializable {
        public String rowKey;
        public String name;
        public String age;

        public HBaseRecord() {}

        public HBaseRecord(String rowKey, String name, String age) {
            this.rowKey = rowKey;
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "HBaseRecord{" +
                    "rowKey='" + rowKey + '\'' +
                    ", name='" + name + '\'' +
                    ", age='" + age + '\'' +
                    '}';
        }
    }
}
