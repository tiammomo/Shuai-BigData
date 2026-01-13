# 大数据 (Big Data) Skills

掌握大数据处理技术栈，包括流处理、消息队列、OLAP 分析和批处理，构建端到端的数据处理流水线。

## 学习路径

1. 先学习 Kafka 作为数据采集层
2. 再学习 Flink/Spark Streaming 进行实时处理
3. 然后学习 ClickHouse/Doris 进行 OLAP 分析
4. 最后学习 Spark 进行批处理和机器学习

## Skills

| Skill | Category | Description |
|-------|----------|-------------|
| [Apache Flink](skills/stream-processing/flink.json) | 流处理 | 分布式流处理引擎，支持精确一次语义、状态管理、窗口操作、Checkpoint |
| [Spark Streaming](skills/stream-processing/spark-streaming.json) | 流处理 | 基于微批处理的流处理框架，与 Spark 生态深度集成 |
| [Apache Kafka](skills/message-queue/kafka.json) | 消息队列 | 高吞吐量分布式消息队列，支持消息持久化和流处理 |
| [ClickHouse](skills/olap/clickhouse.json) | OLAP | 列式存储分析数据库，支持高并发实时 OLAP 查询 |
| [Apache Doris](skills/olap/doris.json) | OLAP | 基于 MPP 架构的分析型数据库，支持高并发实时查询 |
| [Apache Spark](skills/batch-processing/spark.json) | 批处理 | 统一的大数据分析引擎，支持批处理、SQL、流处理和机器学习 |

## 数据流架构

| 架构 | 描述 |
|-----|------|
| 实时数仓 | Kafka -> Flink (ETL) -> Kafka (DW) -> ClickHouse/Doris (OLAP) -> BI |
| 批处理分析 | HDFS/Hive -> Spark (ETL) -> HDFS -> ClickHouse/Doris -> BI |
| Lambda 架构 | 批流一体，兼顾实时和离线 |
| Kappa 架构 | 全链路流处理，统一批流语义 |

## 常见问题

| 问题 | 解决方案 |
|-----|---------|
| 数据倾斜 | Flink: 加盐打散 Key；Spark: 加盐/广播小表；Kafka: 增加分区数 |
| 背压/积压 | 增加并行度，优化算子，检查状态大小 |
| 内存溢出 | 使用 RocksDB 状态后端，调整内存配置 |
