# Delta Lake Spark 集成

## 目录

- [流式操作](#流式操作)
- [批处理](#批处理)
- [性能优化](#性能优化)
- [最佳实践](#最佳实践)

---

## 流式操作

### 写入流

```scala
// 1. 写入流 (追加模式)
val streamDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "events")
  .load()

streamDF.writeStream
  .format("delta")
  .option("checkpointLocation", "/path/to/checkpoint")
  .start("/path/to/delta-sink")

// 2. 写入流 (完整模式)
streamDF.writeStream
  .format("delta")
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("complete")
  .start("/path/to/delta-agg")
```

### 读取流

```scala
// 1. 读取流 (追加模式)
val deltaStream = spark.readStream
  .format("delta")
  .load("/path/to/delta-table")

// 2. 带过滤的流读取
val filteredStream = spark.readStream
  .format("delta")
  .option("startingVersion", 5)  // 从指定版本开始
  .load("/path/to/delta-table")

// 3. 带时间过滤的流读取
val timeFilteredStream = spark.readStream
  .format("delta")
  .option("startingTimestamp", "2024-01-10 00:00:00")
  .load("/path/to/delta-table")
```

### 流批一体

```scala
// 同时支持流式和批量读取
val table = DeltaTable.forPath(spark, "/path/to/delta-table")

// 批量查询
val batchResult = table.toDF.select("id", "value")

// 流式查询
val streamResult = table.toDF
  .writeStream
  .format("console")
  .start()
```

---

## 批处理

### 批量写入

```scala
// 1. 追加模式
df.write
  .format("delta")
  .mode("append")
  .save("/path/to/delta-table")

// 2. 覆盖模式
df.write
  .format("delta")
  .mode("overwrite")
  .save("/path/to/delta-table")

// 3. 覆盖指定数据
df.write
  .format("delta")
  .mode("overwrite")
  .option("replaceWhere", "date >= '2024-01-10' AND date < '2024-01-11'")
  .save("/path/to/delta-table")
```

### 批量读取

```scala
// 1. 读取整个表
val df = spark.read.format("delta").load("/path/to/delta-table")

// 2. 带过滤读取
val filteredDF = spark.read.format("delta")
  .option("versionAsOf", 10)
  .load("/path/to/delta-table")

// 3. SQL 查询
spark.sql("""
  SELECT * FROM delta.`/path/to/delta-table`
  WHERE date >= '2024-01-10'
""")
```

---

## 性能优化

### 文件管理

```scala
// 1. 优化小文件
spark.sql("""
  OPTIMIZE delta.`/path/to/delta-table`
""")

// 2. 优化并清理旧版本
spark.sql("""
  OPTIMIZE delta.`/path/to/delta-table`
  ZORDER BY (date, category)
""")

// 3. 清理旧版本数据
spark.sql("""
  VACUUM delta.`/path/to/delta-table`
  RETAIN 168 HOURS
""")

// 4. 强制清理 (慎用)
spark.sql("""
  VACUUM delta.`/path/to/delta-table` RETAIN 0 HOURS
""")
```

### 分区优化

```scala
// 按日期分区
df.write
  .format("delta")
  .partitionBy("date")
  .save("/path/to/delta-table")

// 多级分区
df.write
  .format("delta")
  .partitionBy("year", "month", "day")
  .save("/path/to/delta-table")
```

### 缓存策略

```scala
// 缓存表
spark.sql("CACHE TABLE delta_table")

// 取消缓存
spark.sql("UNCACHE TABLE delta_table")

// 检查缓存状态
spark.sql("SHOW TABLES").show()
```

---

## 最佳实践

### 1. 分区设计

```scala
// 按时间分区 (推荐)
df.write
  .format("delta")
  .partitionBy("date")  // 或 "year", "month"
  .save("/path/to/delta-table")

// 避免热点分区
// 不要使用低基数列分区
// 如: status (只有几个值)
```

### 2. 文件大小

```scala
// 设置目标文件大小
spark.sql("""
  ALTER TABLE delta_table
  SET TBLPROPERTIES (
    'delta.targetFileSize' = '128MB'
  )
""")
```

### 3. 压缩策略

```scala
// 定期优化
spark.sql("""
  OPTIMIZE delta.`/path/to/delta-table`
  ZORDER BY (id)
""")
```

### 4. 清理策略

```scala
// 设置保留期
spark.sql("""
  ALTER TABLE delta_table
  SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = '7 days'
  )
""")
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作 |
| [README.md](README.md) | 索引文档 |
