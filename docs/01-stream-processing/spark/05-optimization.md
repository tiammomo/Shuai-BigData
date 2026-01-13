# Spark 性能优化指南

## 目录

- [资源配置优化](#资源配置优化)
- [内存管理优化](#内存管理优化)
- [Shuffle 优化](#shuffle-优化)
- [代码优化](#代码优化)
- [数据倾斜处理](#数据倾斜处理)
- [SQL 优化](#sql-优化)

---

## 资源配置优化

### Executor 配置

```scala
// spark-submit 提交参数

// 1. Executor 数量
// --num-executors 3
// 根据数据量和集群资源调整
spark.conf.set("spark.executor.instances", "3")

// 2. 每个 Executor 的 CPU 核心数
// --executor-cores 4
spark.conf.set("spark.executor.cores", "4")

// 3. 每个 Executor 的内存
// --executor-memory 8g
spark.conf.set("spark.executor.memory", "8g")

// 4. Executor 内存溢出时堆外内存
// --conf spark.executor.memoryOverhead=2g
spark.conf.set("spark.executor.memoryOverhead", "2g")

// 5. Driver 内存
// --driver-memory 4g
spark.conf.set("spark.driver.memory", "4g")

// 6. 完整的配置示例
val conf = new SparkConf()
    .set("spark.executor.instances", "10")
    .set("spark.executor.cores", "4")
    .set("spark.executor.memory", "8g")
    .set("spark.executor.memoryOverhead", "2g")
    .set("spark.driver.memory", "4g")
    .set("spark.driver.maxResultSize", "2g")
```

### 并行度配置

```scala
// 1. RDD 并行度
// 数据分区数应与总 CPU 核心数匹配
val rdd = sc.textFile("file.txt", 100)  // 100 个分区

// 2. Shuffle 并行度
spark.conf.set("spark.sql.shuffle.partitions", "200")

// 3. 动态调整并行度
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

// 4. 读取并行度
val df = spark.read
    .option("maxRecordsPerFile", "10000")
    .parquet("path")

// 5. 重新分区
val repartitionedRDD = rdd.repartition(200)  // 强制 Shuffle
val coalescedRDD = rdd.coalesce(50)           // 减少分区(避免 Shuffle)

// 6. 自动调整
spark.conf.set("spark.files.maxPartitionBytes", "128m")  // 每个分区最大字节
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")  // 广播 Join 阈值
```

### 动态资源分配

```scala
// 启用动态资源分配
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "4")
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")

// 资源请求策略
spark.conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "300s")
```

---

## 内存管理优化

### 内存结构

```
┌─────────────────────────────────────────────────────────────────┐
│                  Spark 内存结构                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    执行内存 (Execution)                   │   │
│  │  - Shuffle 排序                                         │   │
│  │  - 广播变量                                             │   │
│  │  - 任务协调                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    存储内存 (Storage)                     │   │
│  │  - RDD 缓存                                             │   │
│  │  - 广播变量                                             │   │
│  │  - 任务结果                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    用户内存 (User)                        │   │
│  │  - 用户数据结构                                          │   │
│  │  - RDD 依赖元数据                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    保留内存 (Reserved)                    │   │
│  │  - 系统预留 (300MB)                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 内存配置

```scala
// 1. 统一内存管理 (默认)
spark.conf.set("spark.memory.useLegacyMode", "false")

// 2. 内存比例配置
spark.conf.set("spark.memory.fraction", "0.6")        // 执行+存储占比
spark.conf.set("spark.memory.storageFraction", "0.5")  // 存储占比

// 3. 堆外内存 (Tungsten)
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")

// 4. 执行内存
spark.conf.set("spark.shuffle.memoryFraction", "0.2")  // Shuffle 内存占比

// 5. RDD 缓存优化
// 根据数据大小选择存储级别
val data = rdd.cache()  // MEMORY_ONLY

// 小数据 - 仅内存
rdd.persist(StorageLevel.MEMORY_ONLY)

// 中等数据 - 内存+磁盘
rdd.persist(StorageLevel.MEMORY_AND_DISK)

// 大数据 - 仅磁盘
rdd.persist(StorageLevel.DISK_ONLY)

// 内存紧张 - 序列化存储
rdd.persist(StorageLevel.MEMORY_ONLY_SER)
rdd.persist(StorageLevel.MEMORY_AND_DISK_SER)

// 6. Kryo 序列化
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryoserializer.buffer.max", "512m")
spark.conf.registerKryoClasses(Array(
    classOf[MyClass1],
    classOf[MyClass2]
))
```

### 垃圾回收优化

```scala
// 1. 垃圾回收器选择
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC " +
    "-XX:G1HeapRegionSize=16m " +
    "-XX:MaxGCPauseMillis=200")

// 2. GC 日志
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+PrintGCDetails " +
    "-XX:+PrintGCDateStamps " +
    "-Xloggc:/path/to/gc.log")

// 3. 降低缓存 RDD 的 GC 影响
// 使用序列化存储
rdd.persist(StorageLevel.MEMORY_ONLY_SER)

// 4. 及时清理
rdd.unpersist()  // 不再使用时清理
```

---

## Shuffle 优化

### Shuffle 配置

```scala
// 1. Shuffle 分区数
spark.conf.set("spark.sql.shuffle.partitions", "200")

// 2. Shuffle 文件缓冲区
spark.conf.set("spark.shuffle.file.buffer", "32k")  // Map 输出缓冲区
spark.conf.set("spark.shuffle.unsafe.file.output.buffer", "5m")  // 文件输出缓冲区

// 3. Reduce 阶段缓冲区
spark.conf.set("spark.reducer.maxSizeInFlight", "48m")  // Reduce 缓冲区大小
spark.conf.set("spark.reducer.maxReqSizeInFlight", "256m")  // 最大请求大小

// 4. Shuffle 合并
spark.conf.set("spark.shuffle.consolidateFiles", "true")  // 合并 Shuffle 文件

// 5. Shuffle 服务
spark.conf.set("spark.shuffle.service.enabled", "true")
spark.conf.set("spark.shuffle.service.port", "7337")

// 6. Shuffle 压缩
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.spill.compress", "true")
```

### 减少 Shuffle

```scala
// 1. 使用 mapPartitions 替代 map
// 不好的写法
rdd.map(item => process(item))

// 好的写法
rdd.mapPartitions(partition => {
    partition.map(item => process(item))
})

// 2. 使用 reduceByKey 替代 groupByKey
// 不好的写法
rdd.groupByKey().mapValues(_.sum)

// 好的写法
rdd.reduceByKey(_ + _)  // 预聚合

// 3. 使用 aggregateByKey 替代 groupByKey
// 不好的写法
rdd.groupByKey().mapValues(values => values.sum)

// 好的写法
rdd.aggregateByKey(0)(_ + _, _ + _)

// 4. 使用 foldByKey 替代 groupByKey
rdd.foldByKey(0)(_ + _)

// 5. 使用 combineByKey 替代 groupByKey
rdd.combineByKey(
    (v) => (v, 1),
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
)

// 6. 广播变量避免 Shuffle
val smallTable = spark.read.table("dim_table").collectAsList()
val broadcastVar = broadcast(smallTable)

val result = largeTable.join(
    broadcastVar.value.as("dim"),
    largeTable("key") === broadcastVar.value("key")
)
```

### Shuffle 排序优化

```scala
// 1. 排序溢出
spark.conf.set("spark.shuffle.spill.numElementsSpillThreshold", "100000")

// 2. 排序算法
spark.conf.set("spark.shuffle.sort.bypassMergeThreshold", "200")

// 3. 减少 Shuffle 数据量
// 使用 filter 提前过滤
val filtered = rdd.filter(_.valid)

// 使用 map 转换减少数据量
val mapped = rdd.map(record => (record.key, record.value))
```

---

## 代码优化

### 避免重复计算

```scala
// 1. 缓存复用 RDD
// 不好的写法
val wordCount1 = textFile.flatMap(_.split(" ")).count()
val wordCount2 = textFile.flatMap(_.split(" ")).count()

// 好的写法
val words = textFile.flatMap(_.split(" "))
words.cache()  // 或 persist
val wordCount1 = words.count()
val wordCount2 = words.count()

// 2. 使用 checkpoint 截断 lineage
ssc.checkpoint("/checkpoint")
val words = textFile.flatMap(_.split(" ")).checkpoint()
```

### 广播变量使用

```scala
// 1. 创建广播变量
val broadcastVar = sc.broadcast(Map("a" -> 1, "b" -> 2))

// 2. 使用广播变量
val result = rdd.map { case (key, value) =>
    val lookup = broadcastVar.value.get(key)
    (key, lookup.getOrElse(0) + value)
}

// 3. 广播大对象
// 广播 DataFrame
val df = spark.read.table("small_table")
val broadcastDF = broadcast(df)

// 4. 更新广播变量
// 需要重新创建广播变量
val newBroadcastVar = sc.broadcast(updatedData)
```

### 序列化优化

```scala
// 1. 使用 Kryo 序列化
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

// 2. 注册类
spark.conf.registerKryoClasses(Array(
    classOf[MyCustomClass],
    classOf[MyOtherClass],
    classOf[Seq[MyCustomClass]]
))

// 3. 调整序列化缓冲区
spark.conf.set("spark.kryoserializer.buffer", "64k")
spark.conf.set("spark.kryoserializer.buffer.max", "512m")

// 4. 使用 Tachyon (Alluxio) 共享缓存
// 减少序列化/反序列化开销
```

### 闭包优化

```scala
// 1. 避免闭包中捕获大对象
// 不好的写法
val largeData = loadLargeData()  // 大对象
rdd.map { item =>
    largeData.lookup(item.key)  // 每次任务都序列化 largeData
}

// 好的写法
val largeData = loadLargeData()
val broadcastData = sc.broadcast(largeData)
rdd.map { item =>
    broadcastData.value.lookup(item.key)
}

// 2. 使用局部变量
rdd.map { item =>
    val localConfig = config  // 复制引用
    processItem(item, localConfig)
}
```

---

## 数据倾斜处理

### 诊断数据倾斜

```scala
// 1. 查看分区数据分布
rdd.mapPartitionsWithIndex { (index, iter) =>
    val startTime = System.currentTimeMillis()
    var count = 0
    iter.foreach { _ => count += 1 }
    Iterator((index, count, System.currentTimeMillis() - startTime))
}.collect().foreach { case (partition, count, time) =>
    println(s"Partition $partition: $count records, $time ms")
}

// 2. 查看 Key 分布
val keyCounts = rdd.map((_, 1)).countByKey()
keyCounts.toSeq.sortBy(-_._2).take(10).foreach(println)

// 3. Spark UI 查看 Stage 耗时
// http://driver:4040
```

### 解决数据倾斜

```scala
// 1. 增加分区数
val repartitioned = rdd.repartition(1000)

// 2. 加盐 Key
// 为热点 Key 添加随机后缀
val saltedRDD = rdd.map { case (key, value) =>
    val salt = Random.nextInt(100)
    (s"$key#$salt", value)
}

// 在聚合后移除盐
val aggregated = saltedRDD
    .reduceByKey(_ + _)
    .map { case (saltedKey, value) =>
        val key = saltedKey.split("#")(0)
        (key, value)
    }
    .reduceByKey(_ + _)

// 3. 两阶段聚合
// 第一阶段: 加盐局部聚合
val localAggregated = rdd
    .map { case (key, value) =>
        val salt = Random.nextInt(10)
        ((key, salt), value)
    }
    .reduceByKey(_ + _)
    .map { case ((key, salt), value) =>
        (key, value)
    }

// 第二阶段: 全局聚合
val result = localAggregated.reduceByKey(_ + _)

// 4. 广播小表 Join
val smallTable = spark.read.table("small_dim").collectAsList()
val broadcastTable = sc.broadcast(smallTable)

val result = largeTable.mapPartitions { partition =>
    val lookup = broadcastTable.value.toMap
    partition.map { case (key, value) =>
        val dim = lookup.getOrElse(key, defaultValue)
        (key, value, dim)
    }
}

// 5. 拆分大 Key 单独处理
val (hotKeys, normalKeys) = rdd.partition { case (key, _) =>
    hotKeySet.contains(key)
}

val hotResult = processHotKeys(hotKeys)
val normalResult = processNormalKeys(normalKeys)
val result = hotResult.union(normalResult)

// 6. 过滤空值
val filtered = rdd.filter(_ != null)
```

---

## SQL 优化

### AQE 优化

```scala
// 1. 启用 AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

// 2. 自动合并小分区
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64m")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")

// 3. Skew Join 优化
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

// 4. 自动选择 SortMergeJoin 或 BroadcastJoin
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")

// 5. 动态切换 Join 策略
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

### 查询优化

```scala
// 1. 谓词下推
// Spark 自动将过滤下推到数据源
df.filter($"date" >= "2024-01-01").select("id", "name")
// 优化为只读取满足条件的数据

// 2. 列裁剪
// 只读取需要的列
df.select("id", "name", "age")

// 3. 分区裁剪
// 读取指定分区
val df = spark.read.parquet("data/year=2024/month=01/")

// 4. 开启谓词下推
spark.conf.set("spark.sql.optimizer.predicatePushdown.enabled", "true")

// 5. 开启列裁剪
spark.conf.set("spark.sql.optimizer.columnPruning.enabled", "true")

// 6. 查看执行计划
df.explain(extended = true)
df.explain(true)

// 7. 开启 Constant Folding
spark.conf.set("spark.sql.optimizer.constantFolding.enabled", "true")

// 8. 开启 Join 重新排序
spark.conf.set("spark.sql.optimizer.joinReorder.enabled", "true")
```

### 缓存优化

```scala
// 1. 缓存 DataFrame
df.cache()
df.persist(StorageLevel.MEMORY_AND_DISK)

// 2. 缓存表
spark.sql("CACHE TABLE table_name")
spark.sql("CACHE LAZY TABLE table_name")

// 3. 查看缓存
spark.sql("SHOW TABLES").show()
spark.table("table_name").storageLevel

// 4. 清除缓存
df.unpersist()
spark.sql("UNCACHE TABLE table_name")
spark.sql("CLEAR CACHE")

// 5. 缓存级别
StorageLevel.MEMORY_ONLY        // 仅内存
StorageLevel.MEMORY_AND_DISK    // 内存+磁盘
StorageLevel.DISK_ONLY          // 仅磁盘
StorageLevel.MEMORY_ONLY_SER    // 序列化
StorageLevel.MEMORY_AND_DISK_SER

// 6. 选择缓存级别
// 小数据: MEMORY_ONLY
// 大数据: MEMORY_AND_DISK
// 内存紧张: MEMORY_ONLY_SER
```

### 自适应查询执行 (AQE)

```scala
// AQE 动态优化
spark.conf.set("spark.sql.adaptive.enabled", "true")

// AQE 自动执行:
// 1. 动态合并 Shuffle 分区
// 2. 动态切换 Join 策略 (SortMergeJoin -> BroadcastJoin)
// 3. 动态处理数据倾斜 (Skew Join)
// 4. 动态优化 Join 顺序

// 查看 AQE 优化
spark.sql("SET spark.sql.adaptive.enabled=true")
spark.sql("EXPLAIN COST SELECT ...")

// 监控 AQE
// Spark UI -> SQL -> Adaptive Query Execution
```

---

## 最佳实践

### 序列化优化

```scala
// 使用 Kryo 序列化 (比 Java 序列化快 10 倍)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "false")
spark.conf.registerKryoClasses(Array(
    classOf[MyClass1],
    classOf[MyClass2]
))
```

### 内存优化

```scala
// Execution Memory vs Storage Memory
// spark.memory.fraction: 0.6 (默认)
// spark.memory.storageFraction: 0.5 (默认)

// 静态内存管理 (老版本)
spark.conf.set("spark.memory.useLegacyMode", "true")
spark.conf.set("spark.executor.memory", "4g")
spark.conf.set("spark.shuffle.memoryFraction", "0.2")
spark.conf.set("spark.storage.memoryFraction", "0.6")
```

### 并行度优化

```scala
// 输入并行度 (HDFS 块数)
spark.conf.set("spark.sql.files.maxPartitionSize", "128m")  // 默认

// Shuffle 并行度
spark.conf.set("spark.sql.shuffle.partitions", "200")  // 默认
spark.conf.set("spark.default.parallelism", "2 * CPU 核心数")  // 默认
```

### Shuffle 优化

```scala
// Shuffle 压缩
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.shuffle.file.buffer", "32k")  // 默认

// Shuffle Read 优化
spark.conf.set("spark.shuffle.io.maxRetries", "3")
spark.conf.set("spark.shuffle.io.retryWait", "5s")

// Shuffle Write 优化
spark.conf.set("spark.shuffle.sort.bypassMergeThreshold", "200")
```

### 广播变量

```scala
// 小表广播
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")  // 默认

// 手动广播
val smallTable = spark.read.table("dim_table").collectAsList()
val broadcastVar = broadcast(smallTable)

val result = largeTable.join(
    broadcastVar.as("dim"),
    largeTable("key") === broadcastVar("key")
)
```

### 缓存优化

```scala
// 缓存级别
// MEMORY_ONLY: 仅内存
// MEMORY_AND_DISK: 内存+磁盘
// MEMORY_ONLY_SER: 序列化后存内存
// DISK_ONLY: 仅磁盘

// 建议: 对重复使用的 DataFrame 使用缓存
df.cache()
df.persist(StorageLevel.MEMORY_AND_DISK)
```

### 避免 Shuffle

```scala
// 使用 mapPartitions 替代 map
// 使用 broadcast 替代 join (小表)
// 使用 reduceByKey 替代 groupByKey
```

### 数据倾斜处理

```scala
// 1. 增加 Shuffle 并行度
df.repartition(1000)

// 2. 广播倾斜 Key
// 识别倾斜 Key，单独处理

// 3. 加盐 Key
df.withColumn("salt", (rand() * 100).cast("int"))
    .withColumn("key", concat(col("key"), col("salt")))
```

### 文件格式选择

```scala
// 推荐: Parquet (列式存储, 压缩率高, 支持 Schema 演进)
// 其他: ORC, Avro
```

### 分区裁剪

```scala
// 尽量使用分区过滤
df.filter(col("date") === "2024-01-10")
```

---

## 常见问题

### Q1: OOM (内存溢出)?

```scala
// 解决:
// 1. 增加 executor 内存
spark.conf.set("spark.executor.memory", "8g")

// 2. 调整内存比例
spark.conf.set("spark.memory.fraction", "0.8")

// 3. 使用磁盘溢出
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")

// 4. 减少并行度
spark.conf.set("spark.sql.shuffle.partitions", "100")

// 5. 序列化优化
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### Q2: Shuffle 数据量大?

```scala
// 解决:
// 1. 增加 Shuffle 并行度
spark.conf.set("spark.sql.shuffle.partitions", "500")

// 2. 使用 reduceByKey 替代 groupByKey
// 3. 广播小表
// 4. 开启 Shuffle 压缩
spark.conf.set("spark.shuffle.compress", "true")
```

### Q3: 数据倾斜?

```scala
// 解决:
// 1. 增加分区数
df.repartition(1000)

// 2. 加盐 Key
df.withColumn("key_salt", concat(col("key"), lit("_"), (rand() * 10).cast("int")))

// 3. 单独处理倾斜 Key
```

### Q4: 执行计划不优化?

```scala
// 解决:
// 1. 开启 AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

// 2. 查看执行计划
df.explain(true)

// 3. 检查数据统计
df.stats()
```

### Q5: 任务执行慢?

```scala
// 解决:
// 1. 增加并行度
spark.conf.set("spark.sql.shuffle.partitions", "400")

// 2. 开启数据压缩
spark.conf.set("spark.rdd.compress", "true")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

// 3. 使用广播 Join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")

// 4. 检查数据倾斜
// 查看 Spark UI 中各任务执行时间
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | Spark 架构详解 |
| [02-spark-sql.md](02-spark-sql.md) | Spark SQL 详解 |
| [03-spark-streaming.md](03-spark-streaming.md) | Spark Streaming 详解 |
| [04-mllib.md](04-mllib.md) | 机器学习库 |
| [README.md](README.md) | 索引文档 |
