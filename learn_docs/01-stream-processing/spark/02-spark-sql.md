# Spark SQL 详解

## 目录

- [核心概念](#核心概念)
- [DataFrame API](#dataframe-api)
- [SQL 查询](#sql-查询)
- [数据源](#数据源)
- [性能优化](#性能优化)

---

## 核心概念

### Spark SQL 简介

```
┌─────────────────────────────────────────────────────────────────┐
│                    Spark SQL 简介                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Spark SQL 是 Spark 的结构化数据处理模块:                        │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ✓ DataFrame/Dataset - 编程接口                          │   │
│  │  ✓ SQL 查询 - 兼容 Hive SQL                              │   │
│  │  ✓ 数据源 API - 多数据源统一访问                          │   │
│  │  ✓ Catalyst 优化器 - 查询优化                             │   │
│  │  ✓ Tungsten 引擎 - 内存管理优化                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  vs Spark Core RDD:                                            │
│                                                                 │
│  | 特性        | Spark SQL        | Spark Core RDD |           │
│  |------------|------------------|----------------|           │
│  | 编程接口    | SQL/DataFrame    | Scala/Java API |           │
│  | Schema     | 自动推断/显式定义  | 无             |           │
│  | 优化       | Catalyst 优化器   | 无             |           │
│  | 执行计划    | 逻辑/物理计划     | DAG            |           │
│  | 内存管理    | Tungsten         | 默认           |           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 核心组件

```scala
// Spark SQL 核心组件

// 1. SparkSession - 入口点
val spark = SparkSession.builder()
    .appName("SparkSQL")
    .config("spark.master", "local[*]")
    .config("spark.sql.warehouse.dir", "/path/to/warehouse")
    .getOrCreate()

// 2. SparkContext (通过 SparkSession 访问)
val sc = spark.sparkContext

// 3. SQLContext (已弃用，使用 SparkSession)
val sqlContext = spark.sqlContext

// 4. DataFrame - 分布式数据集，带 Schema
val df: DataFrame = spark.read.json("data.json")

// 5. Dataset - 强类型 DataFrame
case class Person(name: String, age: Int)
val ds: Dataset[Person] = spark.read.json("data.json").as[Person]
```

---

## DataFrame API

### 创建 DataFrame

```scala
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

// 1. 从 RDD 创建
// 方式1: 自动推断 Schema
val rdd = sc.parallelize(Seq(
    ("Alice", 25, "F"),
    ("Bob", 30, "M"),
    ("Carol", 28, "F")
))
val df1 = rdd.toDF("name", "age", "gender")

// 方式2: 编程定义 Schema
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

val df5 = spark.read.parquet("hdfs://path/to/data.parquet")

// 3. 从 Hive 表创建
val df6 = spark.table("hive_table")

// 4. 从内存数据创建
val data = Seq(
    ("Alice", 25, "F"),
    ("Bob", 30, "M")
)
val df7 = spark.createDataFrame(data).toDF("name", "age", "gender")
```

### 常用操作

```scala
// 1. 查看数据
df.printSchema()  // 打印 Schema
df.show()         // 显示前 20 行
df.show(5, truncate = false)  // 显示 5 行，不截断
df.head()         // 返回 Array[Row]
df.first()        // 返回第一行
df.take(10)       // 返回前 10 行

// 2. 查看信息
df.count()        // 行数
df.columns        // 列名数组
df.dtypes         // 列类型数组
df.schema         // Schema 结构
df.describe().show()  // 统计信息

// 3. 选择列
df.select("name", "age")           // 选择指定列
df.select($"name", $"age" + 1)     // 选择并计算
df.selectExpr("name", "age * 2 as double_age")  // 表达式

// 4. 过滤
df.filter($"age" > 25)             // 条件过滤
df.where($"gender" === "F")        // where 语法
df.filter($"age" > 25 && $"gender" === "F")  // 复合条件

// 5. 排序
df.orderBy($"age".desc)            // 降序
df.sort($"age".asc, $"name".desc)  // 多列排序
df.orderBy(asc("age"))             // 使用 asc/desc 函数

// 6. 分组聚合
df.groupBy("gender").count()       // 分组计数
df.groupBy("gender").avg("age")    // 分组平均
df.groupBy("gender").agg(
    "age" -> "avg",
    "age" -> "max",
    "age" -> "min"
)

// 使用 agg 聚合
import org.apache.spark.sql.functions._
df.groupBy("gender").agg(
    avg($"age").as("avg_age"),
    max($"age").as("max_age"),
    min($"age").as("min_age"),
    count("*").as("count")
)

// 7. 去重
df.distinct()                      // 去重
df.dropDuplicates("gender")        // 按列去重
df.dropDuplicates(Seq("gender", "age"))  // 多列去重

// 8. 限制
df.limit(10)                       // 限制行数

// 9. 采样
df.sample(withReplacement = false, fraction = 0.1)  // 采样 10%

// 10. 列操作
df.withColumn("age_double", $"age" * 2)  // 添加列
df.withColumnRenamed("age", "age_new")   // 重命名列
df.drop("age")                           // 删除列
df.select($"name", $"age".cast("String"))  // 类型转换
```

### 列表达式

```scala
import org.apache.spark.sql.functions._

// 1. 数学函数
df.select(
    abs($"age"),           // 绝对值
    round($"age", 1),      // 四舍五入
    floor($"age"),         // 向下取整
    ceil($"age"),          // 向上取整
    sqrt($"age"),          // 平方根
    pow($"age", 2),        // 幂运算
    $"age" % 10            // 取模
)

// 2. 字符串函数
df.select(
    upper($"name"),               // 大写
    lower($"name"),               // 小写
    trim($"name"),                // 去除空格
    length($"name"),              // 长度
    substring($"name", 1, 3),     // 子字符串
    concat($"name", $"gender"),   // 字符串连接
    concat_ws("-", $"name", $"gender"),  // 带分隔符连接
    split($"name", " "),          // 分割
    regexp_replace($"name", "A", "a"),  // 正则替换
    regexp_extract($"name", "(\\w+)", 1)  // 正则提取
)

// 3. 日期函数
df.select(
    current_date(),                    // 当前日期
    current_timestamp(),               // 当前时间戳
    date_add($"birthday", 10),         // 日期加天数
    date_sub($"birthday", 10),         // 日期减天数
    datediff($"end_date", $"start_date"),  // 日期差
    months_between($"end_date", $"start_date"),  // 月份差
    year($"birthday"),                 // 提取年
    month($"birthday"),                // 提取月
    dayofmonth($"birthday"),           // 提取日
    dayofweek($"birthday"),            // 提取星期
    to_date($"timestamp_col"),         // 转日期
    date_format($"timestamp_col", "yyyy-MM-dd")  // 日期格式化
)

// 4. 条件函数
df.select(
    when($"age" > 30, "adult")
        .when($"age" > 18, "young")
        .otherwise("child")
        .as("age_group")
)

// 5. 空值处理
df.select(
    coalesce($"age", 0),           // 返回第一个非空值
    $"age".isNull,                 // 判断空
    $"age".isNotNull,              // 判断非空
    $"age".na.fill(0),             // 填充空值
    $"age".na.drop                 // 删除空值行
)

// 6. 集合函数
df.select(
    array_contains($"tags", "spark"),  // 数组包含
    explode($"array_col")               // 展开数组
)

// 7. 窗口函数
import org.apache.spark.sql.expressions.Window
val windowSpec = Window.partitionBy("department").orderBy("salary")
df.select(
    $"name",
    $"department",
    $"salary",
    rank().over(windowSpec),           // 排名
    dense_rank().over(windowSpec),     // 密集排名
    row_number().over(windowSpec),     // 行号
    sum($"salary").over(windowSpec),   // 累计和
    avg($"salary").over(windowSpec)    // 累计平均
)
```

---

## SQL 查询

### 注册临时表

```scala
// 1. 创建临时视图
df.createOrReplaceTempView("people")      // 会话级别
df.createGlobalTempView("people")         // 全局级别
df.createTempView("people")               // 抛出异常如果已存在

// 2. 执行 SQL 查询
val result = spark.sql("""
    SELECT
        gender,
        COUNT(*) as count,
        AVG(age) as avg_age,
        MAX(age) as max_age,
        MIN(age) as min_age
    FROM people
    WHERE age >= 18
    GROUP BY gender
    HAVING COUNT(*) > 10
    ORDER BY avg_age DESC
""")

// 3. SQL 函数
spark.sql("""
    SELECT
        name,
        age,
        CASE
            WHEN age < 18 THEN '未成年'
            WHEN age < 30 THEN '青年'
            ELSE '中年'
        END as age_group
    FROM people
""")

// 4. JOIN 查询
spark.sql("""
    SELECT
        o.order_id,
        o.amount,
        c.name,
        c.email
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.amount > 100
""")

// 5. 子查询
spark.sql("""
    SELECT
        name,
        age,
        (SELECT AVG(age) FROM people WHERE gender = p.gender) as gender_avg_age
    FROM people p
""")

// 6. CTE (Common Table Expression)
spark.sql("""
    WITH regional_sales AS (
        SELECT
            region,
            product,
            SUM(amount) as sales
        FROM orders
        GROUP BY region, product
    ),
    top_products AS (
        SELECT
            region,
            product,
            sales
        FROM regional_sales
        WHERE sales > 10000
    )
    SELECT
        region,
        product,
        sales
    FROM top_products
    ORDER BY sales DESC
""")
```

### 内置函数

```scala
// 聚合函数
spark.sql("""
    SELECT
        COUNT(*),                    // 行数
        COUNT(DISTINCT gender),      // 去重计数
        SUM(salary),                 // 求和
        AVG(salary),                 // 平均值
        MIN(salary),                 // 最小值
        MAX(salary),                 // 最大值
        VARIANCE(salary),            // 方差
        STDDEV(salary),              // 标准差
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary)  // 中位数
    FROM employees
""")

// 收集函数
spark.sql("""
    SELECT
        COLLECT_LIST(name) as names,    // 收集为数组
        COLLECT_SET(name) as unique_names  // 收集为去重数组
    FROM employees
    GROUP BY department
""")

// 位运算函数
spark.sql("""
    SELECT
        BIT_AND(flags),    // 位与
        BIT_OR(flags),     // 位或
        BIT_XOR(flags)     // 位异或
    FROM config
    GROUP BY category
""")
```

---

## 数据源

### 通用数据源 API

```scala
// 1. 读取数据
val df = spark.read
    .format("json")                 // 数据格式
    .option("path", "data.json")    // 路径
    .option("header", "true")       // 表头
    .option("inferSchema", "true")  // 推断 Schema
    .option("charset", "UTF-8")     // 字符集
    .option("dateFormat", "yyyy-MM-dd")  // 日期格式
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")  // 时间戳格式
    .option("mode", "PERMISSIVE")   // 解析模式
    .load("path")                   // 加载

// 2. 写入数据
df.write
    .format("parquet")              // 输出格式
    .option("path", "output")       // 输出路径
    .option("compression", "snappy")  // 压缩
    .option("partitionBy", "year", "month")  // 分区
    .mode("overwrite")              // 写入模式
    .save("output")                 // 保存

// 写入模式:
// - append: 追加
// - overwrite: 覆盖
// - errorIfExists: 存在则报错
// - ignore: 忽略
```

### 常用数据源

```scala
// 1. JSON
val jsonDF = spark.read.json(
    "path/to/json",
    "path/to/json2"
)

jsonDF.write
    .format("json")
    .mode("overwrite")
    .save("output/json")

// 2. CSV
val csvDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")       // 分隔符
    .option("quote", "\"")          // 引号字符
    .option("escape", "\\")         // 转义字符
    .option("nullValue", "null")    // 空值表示
    .load("path/to/csv")

// 3. Parquet (推荐)
val parquetDF = spark.read.parquet("path/to/parquet")
parquetDF.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("year", "month")
    .save("output/parquet")

// 4. ORC
val orcDF = spark.read.orc("path/to/orc")
orcDF.write
    .format("orc")
    .mode("overwrite")
    .save("output/orc")

// 5. JDBC
val jdbcDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://host:3306/db")
    .option("dbtable", "schema.table")
    .option("user", "user")
    .option("password", "password")
    .option("fetchsize", "1000")    // 每次读取行数
    .option("partitionColumn", "id")  // 分区列
    .option("lowerBound", "1")      // 分区下界
    .option("upperBound", "10000")  // 分区上界
    .option("numPartitions", "10")  // 并行度
    .load()

// 写入 JDBC
jdbcDF.write
    .format("jdbc")
    .option("url", "jdbc:mysql://host:3306/db")
    .option("dbtable", "table")
    .option("user", "user")
    .option("password", "password")
    .option("batchsize", "1000")    // 批量写入
    .mode("append")
    .save()

// 6. Hive 表
spark.sql("CREATE TABLE IF NOT EXISTS hive_table (...)")
val hiveDF = spark.table("hive_table")
hiveDF.write
    .format("hive")
    .mode("overwrite")
    .saveAsTable("hive_table")

// 7. Text File
val textDF = spark.read.text("path/to/text")
textDF.write
    .format("text")
    .mode("overwrite")
    .save("output/text")
```

### 自定义数据源

```scala
// 1. 实现数据源 API
class CustomSource extends RelationProvider {
    override def createRelation(
        sqlContext: SQLContext,
        parameters: Map[String, String]
    ): BaseRelation = {
        CustomRelation(sqlContext, parameters)
    }
}

case class CustomRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]
) extends BaseRelation with TableScan {
    override def schema: StructType = StructType(Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true)
    ))

    override def buildScan(): RDD[Row] = {
        // 实现数据读取逻辑
        val data = Seq(
            (1, "Alice"),
            (2, "Bob")
        )
        sqlContext.sparkContext.parallelize(data)
            .map(Row.fromTuple)
    }
}

// 2. 使用自定义数据源
spark.read
    .format("com.example.CustomSource")
    .option("param1", "value1")
    .load()
```

---

## 性能优化

### 缓存策略

```scala
// 1. 缓存到内存
df.cache()  // 默认 MEMORY_ONLY
df.persist(StorageLevel.MEMORY_ONLY)

// 2. 内存+磁盘
df.persist(StorageLevel.MEMORY_AND_DISK)

// 3. 序列化存储
df.persist(StorageLevel.MEMORY_ONLY_SER)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

// 4. 磁盘存储
df.persist(StorageLevel.DISK_ONLY)

// 5. 堆外内存
df.persist(StorageLevel.OFF_HEAP)

// 6. 取消缓存
df.unpersist()

// 7. 检查缓存大小
spark.sparkContext.getPersistentRDDs
```

### 广播Join

```scala
// 1. 创建广播变量
val smallTable = spark.read.table("dim_table").collectAsList()
val broadcastVar = broadcast(smallTable)

// 2. 使用广播Join
val result = largeTable.join(
    broadcastVar.value.as("dim"),
    largeTable("key") === broadcastVar.value("key")
)

// 3. 自动广播
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")

// 4. 提示广播
val result = largeTable.join(
    broadcast(smallTable),
    largeTable("key") === smallTable("key")
)
```

### 分区裁剪

```scala
// 1. 静态分区裁剪
val df = spark.read.parquet("data/year=2024/month=01/")
// 读取指定分区的数据

// 2. 动态分区裁剪
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

// 3. 谓词下推
val df = spark.read.parquet("data")
df.filter($"year" === 2024).filter($"month" === 1).show()
// 优化器会将过滤下推到数据源
```

### 查询优化

```scala
// 1. 开启 AQE (Adaptive Query Execution)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

// 2. 开启谓词下推
spark.conf.set("spark.sql.optimizer.predicatePushdown.enabled", "true")

// 3. 开启列裁剪
spark.conf.set("spark.sql.optimizer.columnPruning.enabled", "true")

// 4. 开启 Constant Folding
spark.conf.set("spark.sql.optimizer.constantFolding.enabled", "true")

// 5. 开启 Join 重新排序
spark.conf.set("spark.sql.optimizer.joinReorder.enabled", "true")

// 6. 查看执行计划
df.explain(extended = true)  // 详细计划
df.explain(true)             // 物理计划
df.explain(cost = true)      // 含成本

// 7. 查看 AQE 统计
spark.sql("SET spark.sql.adaptive.enabled=true")
spark.sql("EXPLAIN COST SELECT ...")
```

### 并行度设置

```scala
// 1. Shuffle 并行度
spark.conf.set("spark.sql.shuffle.partitions", "200")

// 2. 读取并行度
val df = spark.read
    .option("maxRecordsPerFile", "10000")
    .parquet("path")

// 3. 写入并行度
df.write
    .bucketBy(100, "id")      // 分桶
    .sortBy("id")
    .saveAsTable("table")

// 4. 分区数量
df.repartition(100)          // 重新分区
df.coalesce(50)              // 合并分区
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | Spark 架构详解 |
| [03-spark-streaming.md](03-spark-streaming.md) | Spark Streaming 详解 |
| [04-mllib.md](04-mllib.md) | 机器学习库 |
| [05-optimization.md](05-optimization.md) | 性能优化指南 |
| [README.md](README.md) | 索引文档 |
