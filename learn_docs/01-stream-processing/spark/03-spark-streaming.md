# Spark Streaming 详解

## 目录

- [核心概念](#核心概念)
- [DStream API](#dstream-api)
- [输入源](#输入源)
- [状态管理](#状态管理)
- [输出操作](#输出操作)
- [性能优化](#性能优化)

---

## 核心概念

### Spark Streaming 简介

```
┌─────────────────────────────────────────────────────────────────┐
│                  Spark Streaming 简介                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Spark Streaming 是 Spark 的流处理模块:                          │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ✓ DStream - 离散流抽象                                  │   │
│  │  ✓ 窗口操作 - 滑动窗口计算                               │   │
│  │  ✓ 状态管理 - 有状态计算                                 │   │
│  │  ✓ 容错机制 - Checkpoint                                │   │
│  │  ✓ 背压控制 - 速率限制                                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  vs 其他流处理框架:                                              │
│                                                                 │
│  | 特性        | Spark Streaming | Flink  | Storm  |           │
│  |------------|-----------------|--------|--------|           │
│  | 处理模型    | 微批处理        | 原生流 | 原生流 |           │
│  | 延迟        | 秒级           | 毫秒级 | 毫秒级 |           │
│  | 容错        | Checkpoint     | Checkpoint | At-least |    │
│  | 状态管理    | 有限           | 丰富   | 无     |           │
│  | 生态        | 与 Spark 整合  | 独立   | 独立   |           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 处理模型

```
┌─────────────────────────────────────────────────────────────────┐
│                  Spark Streaming 处理流程                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  数据源                                                         │
│     │                                                          │
│     ▼                                                          │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Receiver 接收数据                                        │   │
│  │  - 网络数据                                              │   │
│  │  - 消息队列                                              │   │
│  │  - 文件系统                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  批次划分                                                │   │
│  │  - 按时间间隔划分批次                                    │   │
│  │  - 每个批次作为一个 RDD                                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Spark Core 处理                                         │   │
│  │  - RDD 转换操作                                         │   │
│  │  - 批量计算                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          │                                      │
│                          ▼                                      │
│  输出                                                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 核心术语

```scala
// Spark Streaming 核心术语

// 1. StreamingContext - 流处理入口
val ssc = new StreamingContext(sparkConf, Seconds(1))

// 2. DStream - 离散流
// 由连续的 RDD 序列组成
val lines: DStream[String] = ssc.socketTextStream("localhost", 9999)

// 3. 批间隔 (Batch Interval)
// 每个批次的时间长度
val batchInterval = Seconds(1)

// 4. Receiver - 数据接收器
// 从各种源接收数据
// 每个 Receiver 占用一个 Executor Slot

// 5. 检查点 (Checkpoint)
// 定期保存状态，用于容错
ssc.checkpoint("/path/to/checkpoint")

// 6. 窗口 (Window)
// 滑动时间窗口计算
val windowedLines = lines.window(Seconds(10), Seconds(5))
```

---

## DStream API

### 创建 DStream

```scala
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

// 1. StreamingContext
val conf = new SparkConf()
    .setAppName("SparkStreaming")
    .setMaster("local[*]")

val ssc = new StreamingContext(conf, Seconds(1))

// 从 SparkSession 创建
val ssc = SparkSession.builder()
    .appName("SparkStreaming")
    .config("spark.master", "local[*]")
    .getOrCreate()
    .sparkContext
    .streamingContext(Seconds(1))

// 2. Socket 数据源
val lines: DStream[String] = ssc.socketTextStream(
    hostname = "localhost",
    port = 9999,
    storageLevel = StorageLevel.MEMORY_AND_DISK
)

// 3. 文件数据源
val fileStream: DStream[String] = ssc.textFileStream(
    directory = "/path/to/directory",
    filter: Path => !Path.getName.startsWith(".")
)

// 4. RDD 队列 (测试用)
val rddQueue = new Queue[RDD[Int]]()
val queueStream: DStream[Int] = ssc.queueStream(rddQueue)

// 5. 自定义数据源
class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {
    def onStart(): Unit = {
        // 启动接收线程
        new Thread("Custom Receiver") {
            override def run(): Unit = {
                receive()
            }
        }.start()
    }

    def onStop(): Unit = {
        // 停止接收线程
    }

    private def receive(): Unit = {
        // 接收数据逻辑
        while (!isStopped()) {
            val line = readLine()
            store(line)
        }
    }
}

val customStream = ssc.receiverStream(new CustomReceiver())
```

### 转换操作

```scala
// 1. 基础转换
val lines = ssc.socketTextStream("localhost", 9999)
val words = lines.flatMap(_.split(" "))           // 扁平化
val pairs = words.map(word => (word, 1))           // 映射
val wordCounts = pairs.reduceByKey(_ + _)           // 按Key聚合
val filtered = lines.filter(_.contains("error"))   // 过滤

// 2. 窗口转换
val windowedLines = lines.window(Seconds(10))      // 窗口大小 10 秒
val windowedCounts = lines.window(Seconds(10), Seconds(5))  // 窗口 10s, 滑动 5s

// 窗口聚合
val windowedWordCounts = words
    .window(Seconds(10), Seconds(5))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

// 3. 状态转换 (updateStateByKey)
def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = newValues.sum + runningCount.getOrElse(0)
    Some(newCount)
}

val stateDStream = pairs.updateStateByKey(updateFunction)

// 4. 映射转换
val lengths = lines.map(_.length)                  // 简单映射
val pairLines = lines.map(line => (line.split(" ")(0), line))  // Key-Value

// 5. 扁平映射
val tokens = lines.flatMap(_.split(" "))           // 拆分单词
val chars = lines.flatMap(_.toCharArray)           // 拆分字符

// 6. 变换 (transform)
val sortedCounts = wordCounts.transform(rdd => {
    rdd.sortByKey(false)                            // 对每个批次排序
})

// 7. 跨批次操作
val combinedStreams = stream1.union(stream2)       // 合并流
val joinedStreams = stream1.join(stream2)          // Join 两个流
```

### 常用 DStream 操作

```scala
// 1. 计数
lines.count()                                      // 每批次元素数
lines.count().print()

// 2. 打印输出
lines.print()                                      // 打印前 10 行
lines.print(5)                                     // 打印前 5 行

// 3. 遍历
lines.foreachRDD(rdd => {
    rdd.foreachPartition(partition => {
        partition.foreach(record => {
            // 处理每条记录
        })
    })
})

// 4. 采样
lines.sample(withReplacement = false, 0.1)

// 5. 持久化
lines.persist(StorageLevel.MEMORY_AND_DISK)
lines.cache()

// 6. 重新分区
lines.repartition(10)

// 7. 检查点
ssc.checkpoint("/path/to/checkpoint")

// 8. 统计
import org.apache.spark.streaming.util._
val reduced = lines.map((_, 1L)).reduceByKeyAndWindow(
    (a: Long, b: Long) => a + b,                   // 增量计算
    (a: Long, b: Long) => a - b,                   // 窗口滑动减
    Seconds(30),                                   // 窗口大小
    Seconds(10),                                   // 滑动间隔
    2                                              // 并行度
)
```

---

## 输入源

### Socket 数据源

```scala
// 1. 基本 Socket 源
val lines = ssc.socketTextStream("localhost", 9999)

// 2. 带存储级别的 Socket 源
val lines = ssc.socketTextStream(
    "localhost",
    9999,
    StorageLevel.MEMORY_AND_DISK_SER
)

// 3. 监听多个端口
val lines1 = ssc.socketTextStream("host1", 9999)
val lines2 = ssc.socketTextStream("host2", 9998)
val allLines = lines1.union(lines2)
```

### 文件数据源

```scala
// 1. 监控目录
val fileStream = ssc.textFileStream("/data/streaming")

// 2. 监控 HDFS 目录
val hdfsStream = ssc.textFileStream("hdfs://namenode:9000/streaming")

// 3. 自定义过滤
val fileStream = ssc.textFileStream("/data", {
    path: Path => !path.getName.startsWith(".")
})

// 4. 监控多个目录
val stream1 = ssc.textFileStream("/data1")
val stream2 = ssc.textFileStream("/data2")
val allStreams = stream1.union(stream2)

// 注意:
// - 只能监控新文件，不监控已存在文件
// - 文件移动到目录后才开始处理
// - 支持所有 Hadoop compatible 文件系统
```

### 高级数据源

```scala
// 1. Kafka (需要 spark-streaming-kafka)
import org.apache.spark.streaming.kafka._

// Direct 模式 (推荐)
val kafkaParams = Map(
    "bootstrap.servers" -> "localhost:9092",
    "group.id" -> "my-group",
    "auto.offset.reset" -> "latest"
)
val kafkaStream = KafkaUtils.createDirectStream[
    String, String, StringDecoder, StringDecoder
](
    ssc,
    kafkaParams,
    Set("topic1", "topic2")
)

// Receiver 模式
val kafkaStream = KafkaUtils.createStream[
    String, String, StringDecoder, StringDecoder, StringDecoder
](
    ssc,
    kafkaParams,
    Map("topic1" -> 1),  // 每个 Topic 的 Receiver 数
    StorageLevel.MEMORY_AND_DISK
)

// 2. Flume (需要 spark-streaming-flume)
import org.apache.spark.streaming.flume._

// Push 模式
val flumeStream = FlumeUtils.createStream(ssc, "localhost", 41414)

// Pull 模式
val flumeStream = FlumeUtils.createPullStream(ssc, "localhost", 41414)

// 3. Kinesis (需要 spark-streaming-kinesis)
import org.apache.spark.streaming.kinesis._

KinesisUtils.createStream(
    ssc,
    appName = "MyApp",
    streamName = "MyStream",
    endpointUrl = "https://kinesis.us-east-1.amazonaws.com",
    regionName = "us-east-1",
    initialPositionInStream = InitialPositions.TRIM_HORIZON,
    checkpointInterval = Seconds(10),
    storageLevel = StorageLevel.MEMORY_AND_DISK
)

// 4. MQTT (需要 spark-streaming-mqtt)
import org.apache.spark.streaming.mqtt._

val mqttStream = MQTTUtils.createStream(
    ssc,
    "tcp://broker.hivemq.com:1883",
    "topic"
)

// 5. Twitter (需要 spark-streaming-twitter)
import org.apache.spark.streaming.twitter._

TwitterUtils.createStream(
    ssc,
    twitterAuth,
    Seq("keyword1", "keyword2")
)
```

---

## 状态管理

### 有状态转换

```scala
// 1. updateStateByKey
// 维护每个 Key 的全局状态

val lines = ssc.socketTextStream("localhost", 9999)
val words = lines.flatMap(_.split(" "))
val pairs = words.map(word => (word, 1))

// 定义更新函数
def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = newValues.sum + runningCount.getOrElse(0)
    Some(newCount)
}

// 应用状态更新
val stateDStream = pairs.updateStateByKey(updateFunction)
stateDStream.print()

// 2. mapWithState (更高效，Spark 1.6+)
import org.apache.spark.streaming.State
import org.apache.spark.streaming.StateSpec

val mappingFunc = (key: String, value: Option[Int], state: State[Int]) => {
    val sum = value.getOrElse(0) + state.getOption().getOrElse(0)
    val output = (key, sum)
    state.update(sum)
    Some(output)
}

val stateSpec = StateSpec.function(mappingFunc)
val mapStateDStream = pairs.mapWithState(stateSpec)
mapStateDStream.print()

// 3. 可超时状态
val stateSpec = StateSpec.function(mappingFunc)
    .timeout(Seconds(300))  // 300秒超时
```

### 窗口操作

```scala
val lines = ssc.socketTextStream("localhost", 9999)
val words = lines.flatMap(_.split(" "))

// 1. 窗口转换
val windowedWords = words.window(Seconds(10), Seconds(5))
// 窗口大小: 10秒
// 滑动间隔: 5秒

// 2. 窗口聚合
val windowCounts = words
    .window(Seconds(10), Seconds(5))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

// 3. 滑动窗口 reduce
val windowedReduce = words.reduceByKeyAndWindow(
    (a: Int, b: Int) => a + b,    // 添加新数据
    (a: Int, b: Int) => a - b,    // 移除过期数据
    Seconds(30),                  // 窗口大小
    Seconds(10),                  // 滑动间隔
    2                             // 并行度
)

// 4. 窗口计数
val windowLength = Seconds(10)
val slideInterval = Seconds(5)
val windowedCount = words.countByValueAndWindow(windowLength, slideInterval)

// 5. 窗口 join
val stream1 = ssc.socketTextStream("localhost", 9999)
val stream2 = ssc.socketTextStream("localhost", 9998)

val joined = stream1.join(stream2.window(Seconds(10)))
```

### 检查点

```scala
// 1. 启用检查点
ssc.checkpoint("/path/to/checkpoint")

// 2. 检查点间隔
ssc.checkpoint(Duration(10000))  // 10秒

// 3. 从检查点恢复
val ssc = StreamingContext.getOrCreate(
    "/path/to/checkpoint",
    () => createContext()  // 创建新上下文的函数
)

// 4. 检查点保存
// - 元数据检查点
//   - 算子链信息
//   - 未完成的批次信息
// - 数据检查点
//   - 持久化的 DStream
//   - 状态数据

// 5. 检查点示例
def createContext(): StreamingContext = {
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))

    def updateFunction(newValues: Seq[Int], state: Option[Int]): Option[Int] = {
        Some(newValues.sum + state.getOrElse(0))
    }

    val stateDStream = pairs.updateStateByKey(updateFunction)
    stateDStream.checkpoint(Seconds(10))
    stateDStream.print()

    ssc
}

val ssc = StreamingContext.getOrCreate(
    "/checkpoint/path",
    createContext _
)
ssc.start()
ssc.awaitTermination()
```

---

## 输出操作

### 输出到外部系统

```scala
// 1. 打印
wordCounts.print()           // 打印到控制台
wordCounts.print(10)         // 打印前10行

// 2. 写入文件
wordCounts.saveAsTextFiles("/output/wordcount", "txt")
wordCounts.saveAsHadoopFiles("/output", "txt")  // Hadoop 格式

// 3. 写入 HDFS
wordCounts.saveAsTextFiles("hdfs://namenode:9000/output/wordcount")

// 4. 保存为对象文件
wordCounts.saveAsObjectFiles("/output/object", "obj")

// 5. 写入数据库
wordCounts.foreachRDD(rdd => {
    rdd.foreachPartition(partition => {
        val conn = getDBConnection()  // 获取数据库连接
        partition.foreach { case (word, count) =>
            // 写入数据库
            upsertWordCount(conn, word, count)
        }
        conn.close()
    })
})

// 6. 写入 Redis
import redis.clients.jedis.Jedis

wordCounts.foreachRDD(rdd => {
    rdd.foreachPartition(partition => {
        val jedis = new Jedis("localhost", 6379)
        partition.foreach { case (word, count) =>
            jedis.hincrBy("wordcount", word, count)
        }
        jedis.close()
    })
})

// 7. 写入 Kafka
import org.apache.spark.streaming.kafka.KafkaUtils

val kafkaDStream = wordCounts.map {
    case (word, count) => (word, count.toString)
}

kafkaDStream.foreachRDD(rdd => {
    rdd.foreachPartition(partition => {
        val producer = createKafkaProducer()
        partition.foreach { case (word, count) =>
            producer.send(new ProducerRecord("output-topic", word, count))
        }
        producer.close()
    })
})
```

### 事务写入

```scala
// 确保数据精确一次写入
wordCounts.foreachRDD { (rdd, time) =>
    // 每个批次只创建一次连接
    val jedis = new Jedis("localhost", 6379)

    // 使用事务
    val pipeline = jedis.pipelined()
    pipeline.multi()

    rdd.foreachPartition { partition =>
        partition.foreach { case (word, count) =>
            pipeline.hincrBy("wordcount", word, count)
        }
    }

    pipeline.exec()
    pipeline.sync()
    jedis.close()
}

// 幂等写入
wordCounts.foreachRDD { (rdd, time) =>
    rdd.foreachPartition { partition =>
        val conn = getDBConnection()
        partition.foreach { case (word, count) =>
            // 使用 UPSERT 语义
            upsertWordCount(conn, word, count)
        }
        conn.close()
    }
}
```

---

## 性能优化

### 背压控制

```scala
// 1. 启用背压
sparkConf.set("spark.streaming.backpressure.enabled", "true")

// 2. 初始速率
sparkConf.set("spark.streaming.backpressure.initialRate", "1000")

// 3. 速率估计器
sparkConf.set("spark.streaming.receiver.maxRate", "10000")    // Receiver 模式
sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1000")  // Direct 模式

// 4. 限制最大速率
val stream = KafkaUtils.createDirectStream[
    String, String, StringDecoder, StringDecoder
](
    ssc,
    kafkaParams,
    Map("topic" -> 1),
    MessageHandler
)
stream.map(_._2)
    .transform { rdd =>
        rdd.mapPartitions { partition =>
            // 限制每分区处理速率
            partition.take(1000)
        }
    }
```

### 并行度优化

```scala
// 1. Receiver 并行度
// 多个 Receiver 接收数据
val stream1 = ssc.socketTextStream("host1", 9999)
val stream2 = ssc.socketTextStream("host2", 9998)
val allStreams = stream1.union(stream2)

// 2. Receiver 数量
val numReceivers = 3
(1 to numReceivers).map { i =>
    ssc.socketTextStream(s"host$i", 9999 + i)
}.reduce(_ union _)

// 3. 算子并行度
val pairs = words.map(word => (word, 1)).reduceByKey(_ + _, 10)

// 4. Shuffle 并行度
ssc.sparkContext.setJobGroup("group1", "WordCount Job")
pairs.reduceByKey(_ + _, 100)  // 100 个分区

// 5. 窗口并行度
val windowedCounts = words
    .window(Seconds(10), Seconds(5))
    .map(word => (word, 1))
    .reduceByKey(_ + _, 10)  // 指定分区数
```

### 内存优化

```scala
// 1. 序列化
sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
sparkConf.registerKryoClasses(Array(
    classOf[MyClass1],
    classOf[MyClass2]
))

// 2. 批处理大小
sparkConf.set("spark.streaming.batchDuration", "5000")  // 5秒

// 3. Receiver 存储级别
val stream = ssc.socketTextStream(
    "localhost",
    9999,
    StorageLevel.MEMORY_AND_DISK_SER
)

// 4. 窗口大小
// 根据数据量和延迟要求调整
val windowedStream = lines.window(Seconds(30), Seconds(10))

// 5. 检查点间隔
ssc.checkpoint(Seconds(60))  // 1分钟
```

### 监控与调优

```scala
// 1. 度量配置
val ssc = new StreamingContext(conf, Seconds(1))
ssc.addStreamingListener(new StreamingListener {
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
        val info = batchCompleted.batchInfo
        println(s"Batch completed: ${info.batchTime}")
        println(s"Scheduling delay: ${info.schedulingDelay.getOrElse(0)}ms")
        println(s"Processing time: ${info.processingDelay.getOrElse(0)}ms")
        println(s"Total delay: ${info.totalDelay.getOrElse(0)}ms")
    }
})

// 2. 任务配置
sparkConf.set("spark.speculation", "true")  // 启用推测执行
sparkConf.set("spark.task.cpus", "1")
sparkConf.set("spark.executor.cores", "2")  // 每个 Executor 2 核
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | Spark 架构详解 |
| [02-spark-sql.md](02-spark-sql.md) | Spark SQL 详解 |
| [04-mllib.md](04-mllib.md) | 机器学习库 |
| [05-optimization.md](05-optimization.md) | 性能优化指南 |
| [README.md](README.md) | 索引文档 |
