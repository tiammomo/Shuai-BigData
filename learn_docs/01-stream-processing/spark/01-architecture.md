# Apache Spark 架构详解

## 目录

- [核心概念](#核心概念)
- [架构组件](#架构组件)
- [执行模型](#执行模型)
- [RDD 详解](#rdd-详解)
- [DataFrame/Dataset](#dataframedataset)
- [性能优化](#性能优化)

---

## 核心概念

### Spark 简介

```
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Spark 简介                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Apache Spark 是一个统一的大数据分析处理引擎:                     │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ✓ Spark Core - 基础计算引擎                             │   │
│  │  ✓ Spark SQL - 结构化数据处理                            │   │
│  │  ✓ Spark Streaming - 实时流处理                          │   │
│  │  ✓ Spark MLlib - 机器学习                                │   │
│  │  ✓ Spark GraphX - 图计算                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  vs Hadoop MapReduce:                                           │
│                                                                 │
│  | 特性        | Spark        | MapReduce |                     │
│  |------------|--------------|-----------|                     │
│  | 内存计算    | 支持         | 不支持    |                     │
│  | 迭代计算    | 高效         | 低效     |                     │
│  | 延迟       | 毫秒级        | 秒级     |                     │
│  | 容错       | Lineage      | 多副本    |                     │
│  | 编程模型    | RDD          | MapReduce |                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 核心术语

```scala
// Spark 核心术语

// 1. Application - 应用程序
// 包含 Driver 和 Executors

// 2. Driver - 驱动程序
// main() 函数，创建 SparkContext

// 3. SparkContext - Spark 上下文
// 连接集群，创建 RDD

// 4. Cluster Manager - 集群管理器
// YARN, Mesos, Kubernetes, Standalone

// 5. Executor - 执行器
// 运行在 Worker 节点，执行任务

// 6. Task - 任务
// 执行器中的最小工作单元

// 7. Job - 作业
// 由多个 Stage 组成

// 8. Stage - 阶段
// 由多个 Task 组成

// 9. RDD - 弹性分布式数据集
// 分布式内存抽象

// 10. DAG - 有向无环图
// 作业执行计划
```

---

## 架构组件

### 集群架构

```
┌─────────────────────────────────────────────────────────────────────────�│
│                          Spark 集群架构                                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      Client                                      │   │
│  │  - 提交 Spark 应用                                              │   │
│  │  - 包含 Driver 程序                                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                  Cluster Manager (集群管理器)                     │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐           │   │
│  │  │ Standalone│  │  YARN   │  │ Mesos   │  │K8s     │           │   │
│  │  │ Master  │  │  RM     │  │ Master  │  │ Operator│           │   │
│  │  │         │  │         │  │         │  │         │           │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘           │   │
│  │       │              │              │              │            │   │
│  │       └──────────────┼──────────────┼──────────────┘            │   │
│  │                      ▼              ▼                             │   │
│  └──────────────────────┼──────────────┼────────────────────────────┘   │
│                         │              │                                │
│                         ▼              ▼                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      Worker Node (工作节点)                      │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │   │
│  │  │  Executor  │  │  Executor  │  │  Executor  │            │   │
│  │  │  (Core 1)  │  │  (Core 2)  │  │  (Core 3)  │            │   │
│  │  │  ┌───────┐ │  │  ┌───────┐ │  │  ┌───────┐ │            │   │
│  │  │  │ Task  │ │  │  │ Task  │ │  │  │ Task  │ │            │   │
│  │  │  │ Task  │ │  │  │ Task  │ │  │  │ Task  │ │            │   │
│  │  │  └───────┘ │  │  └───────┘ │  │  └───────┘ │            │   │
│  │  │  Cache    │  │  Cache    │  │  Cache    │            │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘            │   │
│  │                                                                  │   │
│  │  - 任务执行     - 数据缓存     - 结果返回                       │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 执行流程

```scala
// Spark 执行流程

// 1. 创建 SparkContext
val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
val sc = new SparkContext(conf)

// 2. 加载数据
val lines = sc.textFile("hdfs://input.txt")

// 3. 转换操作
val words = lines.flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))
val counts = pairs.reduceByKey(_ + _)

// 4. 行动操作
val result = counts.collect()

// 执行流程:
// ┌─────────────────────────────────────────────────────────────────┐
// │  Step 1: Driver 创建 SparkContext                               │
// │  Step 2: SparkContext 连接 Cluster Manager                      │
// │  Step 3: 申请资源，分配 Executor                                 │
// │  Step 4: Executor 注册到 Driver                                  │
// │  Step 5: 提交作业，划分为 Stage 和 Task                          │
// │  Step 6: Task 分发到 Executor 执行                               │
// │  Step 7: Executor 发送心跳和结果                                 │
// │  Step 8: Driver 汇总结果                                         │
// └─────────────────────────────────────────────────────────────────┘
```

---

## 执行模型

### DAG 和 Stage

```
┌─────────────────────────────────────────────────────────────────┐
│                    DAG 和 Stage 划分                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  原始 DAG:                                                       │
│                                                                 │
│  textFile ──► flatMap ──► map ──► reduceByKey ──► save          │
│       │                        │                                 │
│       │                        ▼                                 │
│       │                  partitionBy                            │
│       │                                                       │
│       ▼                                                       │
│  Shuffle ─────────────────────────────►                         │
│       │                                                       │
│       ▼                                                       │
│  Stage 划分:                                                    │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Stage 0                                                │   │
│  │  ────────────                                            │   │
│  │  textFile → flatMap → map → partitionBy                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                              │                                  │
│                              ▼ (Shuffle)                        │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Stage 1                                                │   │
│  │  ────────────                                            │   │
│  │  ShuffleRead → reduceByKey → save                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  规则:                                                          │
│  - 宽依赖 (Shuffle) 触发 Stage 划分                              │
│  - 窄依赖在同一 Stage 内                                         │
│  - Stage 按拓扑顺序执行                                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 宽依赖 vs 窄依赖

```scala
// 窄依赖 (Narrow Dependency)
// 父 RDD 的每个分区只被子 RDD 的一个分区使用

// 窄依赖操作:
map, filter, union, sample, cartesian, intersection, subtract, zip

// 宽依赖 (Wide Dependency / Shuffle)
// 父 RDD 的每个分区被子 RDD 的多个分区使用

// 宽依赖操作:
groupByKey, reduceByKey, join, cogroup, repartition, coalesce(with shuffle)

// 依赖关系图:
val rdd1 = sc.parallelize(1 to 100)
val rdd2 = rdd1.map(_ * 2)          // 窄依赖
val rdd3 = rdd2.filter(_ > 50)      // 窄依赖
val rdd4 = rdd3.groupByKey()        // 宽依赖 (Shuffle)
val rdd5 = rdd4.mapValues(_.sum)    // 窄依赖
```

---

## RDD 详解

### RDD 操作

```scala
import org.apache.spark.rdd.RDD

// 1. 创建 RDD
// 从集合
val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
val rdd2 = sc.makeRDD(Seq(1, 2, 3, 4, 5))

// 从外部存储
val rdd3 = sc.textFile("hdfs://path/to/file.txt")
val rdd4 = sc.sequenceFile[Int, String]("hdfs://path/to/sequence")

// 从其他 RDD 转换
val rdd5 = rdd1.map(_ * 2)

// 2. 转换操作 (Lazy)
val lines = sc.textFile("input.txt")
val words = lines.flatMap(_.split(" "))  // 窄依赖
val pairs = words.map((_, 1))            // 窄依赖
val wordCounts = pairs.reduceByKey(_ + _) // 宽依赖

// 3. 行动操作 (Eager)
wordCounts.count()        // 计数
wordCounts.collect()      // 收集到 Driver
wordCounts.saveAsTextFile("output")  // 保存
wordCounts.take(10)       // 取前 10 个
wordCounts.first()        // 第一个

// 4. 持久化
// 缓存到内存
wordCounts.persist(StorageLevel.MEMORY_ONLY)
wordCounts.cache()  // 默认 MEMORY_ONLY

// 多种存储级别
StorageLevel.MEMORY_ONLY        // 仅内存
StorageLevel.MEMORY_AND_DISK    // 内存+磁盘
StorageLevel.DISK_ONLY          // 仅磁盘
StorageLevel.MEMORY_ONLY_SER    // 序列化存储
StorageLevel.OFF_HEAP           // 堆外内存

// 取消缓存
wordCounts.unpersist()
```

### 分区控制

```scala
// 1. 查看分区数
rdd.getNumPartitions  // 8

// 2. 设置分区数
val rdd1 = sc.textFile("file.txt", 10)  // 10 个分区

// 3. 重新分区
val rdd2 = rdd1.repartition(20)  // 重新分区 (Shuffle)
val rdd3 = rdd1.coalesce(5)      // 合并分区 (可避免 Shuffle)

// 4. 自定义分区
val rdd4 = rdd1.partitionBy(new HashPartitioner(10))

// 5. 获取分区数据
val partitions = rdd.glom().collect()  // 每个分区的数据
```

---

## DataFrame/Dataset

### 创建 DataFrame

```scala
import org.apache.spark.sql._

// 1. 从 RDD 创建
val rdd = sc.parallelize(Seq(
    ("Alice", 25, "F"),
    ("Bob", 30, "M"),
    ("Carol", 28, "F")
))

// 方式1: 通过反射推断 Schema
val df1 = rdd.toDF("name", "age", "gender")

// 方式2: 编程指定 Schema
import org.apache.spark.sql.types._
val schema = StructType(Array(
    StructField("name", StringType, nullable = false),
    StructField("age", IntegerType, nullable = true),
    StructField("gender", StringType, nullable = true)
))
val df2 = spark.createDataFrame(rdd, schema)

// 2. 从数据源创建
val df3 = spark.read.format("json")
    .load("hdfs://path/to/data.json")

val df4 = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://path/to/data.csv")

// JDBC 数据源
val df5 = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://host:3306/db")
    .option("dbtable", "table")
    .option("user", "user")
    .option("password", "password")
    .load()

// 3. DSL 风格查询
df.select("name", "age")
  .filter("age > 25")
  .groupBy("gender")
  .agg(avg("age"), count("*"))
  .orderBy("gender")
  .show()

// 4. SQL 风格查询
df.createOrReplaceTempView("people")
spark.sql("""
    SELECT gender, avg(age) as avg_age, count(*) as cnt
    FROM people
    WHERE age > 25
    GROUP BY gender
    ORDER BY gender
""").show()
```

### Dataset 操作

```scala
// 强类型 Dataset
case class Person(name: String, age: Int, gender: String)

// 创建 Dataset
val ds1: Dataset[Person] = spark.read
    .json("people.json")
    .as[Person]

// 转换操作
val names = ds1.map(_.name.toUpperCase)
val adults = ds1.filter(_.age >= 18)

// 与 DataFrame 互转
val df = ds1.toDF()           // Dataset -> DataFrame
val ds2 = df.as[Person]       // DataFrame -> Dataset

// 聚合操作
import org.apache.spark.sql.functions._
ds1.groupBy("gender")
   .agg(
       avg("age"),
       count("*"),
       sum("age")
   )
   .show()
```

---

## 性能优化

### 内存管理

```scala
// 1. 内存配置
// spark-submit 配置
// --executor-memory 8g
// --driver-memory 4g

// 代码配置
val conf = new SparkConf()
conf.set("spark.executor.memory", "8g")
conf.set("spark.driver.memory", "4g")
conf.set("spark.memory.fraction", "0.6")  // 执行/存储内存比例
conf.set("spark.memory.storageFraction", "0.5")  // 存储内存占比

// 2. 内存管理策略
conf.set("spark.memory.useLegacyMode", "false")  // 统一内存管理
conf.set("spark.executor.memoryOverhead", "2g")  // 堆外内存

// 3. RDD 缓存优化
// 根据数据大小选择存储级别
rdd.persist(StorageLevel.MEMORY_ONLY)      // 小数据
rdd.persist(StorageLevel.MEMORY_AND_DISK)  // 中等数据
rdd.persist(StorageLevel.DISK_ONLY)        // 大数据
rdd.persist(StorageLevel.MEMORY_ONLY_SER)  // 内存紧张
```

### Shuffle 优化

```scala
// 1. Shuffle 配置
conf.set("spark.sql.shuffle.partitions", "200")  // 默认 200
conf.set("spark.reducer.maxSizeInFlight", "48m")  // Reduce 缓冲区
conf.set("spark.shuffle.file.buffer", "32k")      // Map 输出缓冲区
conf.set("spark.shuffle.unsafe.file.output.buffer", "5m")  // 文件输出缓冲区

// 2. 广播变量
// 小表广播，避免 Shuffle
val smallTable = spark.read.table("dim_table").collectAsList()
val broadcastVar = sc.broadcast(smallTable)

val result = largeTable.join(
    broadcastVar.value.as("dim"),
    largeTable("key") === broadcastVar.value("key")
)

// 3. 减少 Shuffle
// 使用 mapPartitions 替代 map
rdd.mapPartitions(iter => {
    // 每个分区批量处理
    iter.map(...)
})

// 使用 reduceByKey 替代 groupByKey
// 不好的写法
rdd.groupByKey().mapValues(_.sum)

// 好的写法
rdd.reduceByKey(_ + _)  // 预聚合
```

### 代码优化

```scala
// 1. 避免重复创建 RDD
// 不好的写法
val wordCount1 = textFile.flatMap(_.split(" ")).count()
val wordCount2 = textFile.flatMap(_.split(" ")).count()

// 好的写法
val words = textFile.flatMap(_.split(" "))
val wordCount1 = words.count()
val wordCount2 = words.count()

// 2. 使用持久化
// 多次使用 RDD 时持久化
val processed = data.map(...).filter(...)
processed.count()      // 第一次计算并缓存
processed.collect()    // 使用缓存

// 3. 使用合适的数据结构
// 使用 MapType 替代嵌套结构
// 使用编码器进行序列化

// 4. 局部变量
// 避免闭包中捕获大对象
val largeObject = ...
rdd.map { item =>
    val local = largeObject  // 复制引用
    item.process(local)
}

// 5. 并行度设置
// 根据数据量和集群资源
val rdd = sc.textFile("bigfile.txt", 100)  // 100 分区
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [02-spark-sql.md](02-spark-sql.md) | Spark SQL 详解 |
| [03-spark-streaming.md](03-spark-streaming.md) | Spark Streaming 详解 |
| [04-mllib.md](04-mllib.md) | 机器学习库 |
| [05-optimization.md](05-optimization.md) | 性能优化指南 |
| [README.md](README.md) | 索引文档 |
