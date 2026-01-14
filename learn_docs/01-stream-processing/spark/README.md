# Spark 中文文档

> Apache Spark 是一个统一的大数据分析处理引擎，支持批处理、交互式查询、流处理和机器学习。

## 文档列表

| 文档 | 说明 | 关键内容 |
|------|------|---------|
| [01-architecture.md](01-architecture.md) | 架构详解 | RDD、DAG、Stage、内存模型 |
| [02-spark-sql.md](02-spark-sql.md) | Spark SQL 详解 | DataFrame、SQL 查询、数据源 |
| [03-spark-streaming.md](03-spark-streaming.md) | Spark Streaming 详解 | DStream、窗口操作、检查点 |
| [04-mllib.md](04-mllib.md) | 机器学习库 | 分类、聚类、推荐、管道 |
| [05-optimization.md](05-optimization.md) | 性能优化指南 | 内存、Shuffle、数据倾斜 |

## 快速入门

### 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     Spark 核心概念                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Application (应用程序)                                          │
│  └── Driver (驱动程序)                                           │
│      └── SparkContext                                           │
│          └── RDD (弹性分布式数据集)                               │
│              └── Transformations (转换)                          │
│              └── Actions (动作)                                  │
│                                                                 │
│  Cluster Manager (集群管理器)                                     │
│  ├── YARN                                                       │
│  ├── Mesos                                                      │
│  ├── Kubernetes                                                 │
│  └── Standalone                                                 │
│                                                                 │
│  Worker Node (工作节点)                                          │
│  └── Executor (执行器)                                           │
│      └── Task (任务)                                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 代码示例

```scala
// 1. 创建 SparkSession
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
    .appName("WordCount")
    .config("spark.master", "local[*]")
    .getOrCreate()

// 2. RDD 操作
val textFile = spark.sparkContext.textFile("hdfs://input.txt")
val wordCounts = textFile
    .flatMap(_.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .collect()

// 3. DataFrame 操作
val df = spark.read.json("data.json")
df.createOrReplaceTempView("people")
val adults = spark.sql("SELECT * FROM people WHERE age >= 18")

// 4. Streaming 操作
import org.apache.spark.streaming._

val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
val lines = ssc.socketTextStream("localhost", 9999)
val wordCounts = lines.flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _)
wordCounts.print()
ssc.start()
```

## 文档导航

### 入门

1. 先阅读 [01-architecture.md](01-architecture.md) 了解 Spark 核心架构
2. 学习 [02-spark-sql.md](02-spark-sql.md) 掌握 DataFrame 和 SQL 操作

### 流处理

3. 了解 [03-spark-streaming.md](03-spark-streaming.md) 学习流处理

### 机器学习

4. 学习 [04-mllib.md](04-mllib.md) 掌握机器学习算法

### 优化

5. 参考 [05-optimization.md](05-optimization.md) 进行性能调优

## 版本兼容性

| Spark 版本 | Scala 版本 | Hadoop 版本 |
|-----------|-----------|------------|
| 3.5.x | 2.12/2.13 | 3.3+ |
| 3.4.x | 2.12/2.13 | 3.3+ |
| 3.3.x | 2.12 | 3.3+ |
| 3.2.x | 2.12 | 3.3+ |

## 生态组件

```
┌─────────────────────────────────────────────────────────────────┐
│                        Spark 生态                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Spark Core                            │   │
│  │  - RDD 抽象                                              │   │
│  │  - 任务调度                                             │   │
│  │  - 内存管理                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐   │
│  │  Spark SQL  │ Spark Str.  │  MLlib      │  GraphX     │   │
│  │  结构化数据 │ 流处理      │ 机器学习    │ 图计算      │   │
│  └─────────────┴─────────────┴─────────────┴─────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    集群管理器                             │   │
│  │  YARN / Mesos / Kubernetes / Standalone                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 最佳实践

### 开发规范

```scala
// 1. 使用 DataFrame/Dataset 替代 RDD (除非需要)
val df = spark.read.parquet("data")

// 2. 及时缓存复用 RDD
val processed = data.map(...).filter(...)
processed.cache()
processed.count()
processed.collect()

// 3. 使用广播变量
val smallTable = spark.read.table("dim").collectAsList()
val broadcastVar = spark.sparkContext.broadcast(smallTable)

// 4. 避免宽依赖
// 使用 reduceByKey 替代 groupByKey
// 使用 mapPartitions 替代 map

// 5. 序列化选择 Kryo
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### 部署配置

```bash
# spark-submit 常用配置
spark-submit \
    --class com.example.App \
    --master yarn \
    --deploy-mode cluster \
    --executor-memory 8g \
    --executor-cores 4 \
    --num-executors 10 \
    --driver-memory 4g \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --jars jars/*.jar \
    app.jar
```

## 相关资源

- [官方文档](https://spark.apache.org/documentation.html)
- [Spark SQL 文档](https://spark.apache.org/sql/)
- [Spark Streaming 文档](https://spark.apache.org/streaming/)
- [MLlib 文档](https://spark.apache.org/mllib/)
