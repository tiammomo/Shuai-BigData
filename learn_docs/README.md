# 学习文档索引

本文档体系按照技术领域组织，提供了完整的大数据技术学习路径。

## 目录结构

```
learn_docs/
├── 00-overview/                    # 概览与架构
│   ├── ai-llm-scenarios.md        # AI/LLM 与数据湖集成场景
│   ├── architecture-diagrams.md   # 架构图集
│   └── docker-integration.md      # Docker 环境集成
│
├── 01-stream-processing/          # 流处理引擎
│   ├── flink/                     # Apache Flink
│   │   ├── 01-architecture.md     # 架构与核心概念
│   │   ├── 02-datastream.md       # DataStream API 详解
│   │   ├── 03-table-sql.md        # Table API 与 Flink SQL
│   │   ├── 04-state-checkpoint.md # 状态管理与 Checkpoint
│   │   ├── 05-cep.md              # 复杂事件处理 CEP
│   │   └── 06-operations.md       # 部署与运维
│   │
│   ├── fluss/                     # Apache Fluss
│   │   ├── 01-architecture.md     # Fluss 架构解析
│   │   └── README.md              # Fluss 使用指南
│   │
│   └── spark/                     # Apache Spark
│       ├── 01-architecture.md     # Spark 核心架构
│       ├── 02-spark-sql.md        # Spark SQL 详解
│       ├── 03-spark-streaming.md  # Spark Streaming
│       ├── 04-mllib.md            # MLlib 机器学习
│       └── 05-optimization.md     # 性能优化
│
├── 02-message-queue/              # 消息队列
│   ├── kafka/                     # Apache Kafka
│   │   ├── 01-architecture.md     # Kafka 架构
│   │   ├── 02-producer.md         # 生产者详解
│   │   ├── 03-consumer.md         # 消费者与组
│   │   ├── 04-streams.md          # Kafka Streams
│   │   ├── 05-operations.md       # 运维与监控
│   │   └── 06-troubleshooting.md  # 问题排查
│   │
│   └── pulsar/                    # Apache Pulsar
│       ├── 01-architecture.md     # Pulsar 架构
│       ├── 02-producer.md         # 生产者
│       ├── 03-consumer.md         # 消费者
│       ├── 04-functions.md        # Pulsar Functions
│       └── README.md              # 使用指南
│
├── 03-olap/                       # OLAP 数据库
│   ├── clickhouse/                # ClickHouse
│   │   ├── 01-architecture.md     # 架构与 MergeTree
│   │   ├── 02-merge-tree.md       # MergeTree 详解
│   │   ├── 03-data-operations.md  # 数据操作
│   │   └── 04-optimization.md     # 性能优化
│   │
│   ├── doris/                     # Apache Doris
│   │   ├── 01-architecture.md     # 架构与节点
│   │   ├── 02-data-model.md       # 数据模型
│   │   ├── 03-data-operations.md  # 数据操作
│   │   ├── 04-sql-reference.md    # SQL 参考
│   │   └── 05-operations.md       # 运维操作
│   │
│   ├── druid/                     # Apache Druid
│   │   ├── 01-architecture.md     # Druid 架构
│   │   ├── 02-data-operations.md  # 数据操作
│   │   └── README.md              # 使用指南
│   │
│   ├── kylin/                     # Apache Kylin
│   │   ├── 01-architecture.md     # Kylin 架构
│   │   ├── 02-usage.md            # 使用指南
│   │   └── README.md              # 快速开始
│   │
│   └── presto-trino/              # Presto / Trino
│       ├── 01-architecture.md     # 架构解析
│       ├── 02-usage.md            # 使用指南
│       └── README.md              # 快速开始
│
├── 04-data-lake/                  # 数据湖与表格式
│   ├── paimon/                    # Apache Paimon
│   │   ├── 01-architecture.md     # Paimon 架构
│   │   ├── 02-usage.md            # 使用指南
│   │   └── 03-spark-sql.md        # Spark SQL 操作
│   │
│   ├── hudi/                      # Apache Hudi
│   │   ├── 01-architecture.md     # Hudi 架构
│   │   ├── 02-data-operations.md  # 数据操作
│   │   ├── 03-flink-integration.md # Flink 集成
│   │   └── 04-spark-sql.md        # Spark SQL 操作
│   │
│   ├── iceberg/                   # Apache Iceberg
│   │   ├── 01-architecture.md     # Iceberg 架构
│   │   ├── 02-data-operations.md  # 数据操作
│   │   ├── 03-flink-integration.md # Flink 集成
│   │   └── 04-spark-sql.md        # Spark SQL 操作
│   │
│   └── delta-lake/                # Delta Lake
│       ├── 01-architecture.md     # 架构解析
│       ├── 02-usage.md            # 使用指南
│       ├── 03-spark-sql.md        # Spark SQL 操作
│       └── 04-flink-sql.md        # Flink SQL 操作
│
├── 05-nosql/                      # NoSQL 数据库
│   ├── redis/                     # Redis
│   │   ├── 01-architecture.md     # Redis 架构
│   │   ├── 02-data-operations.md  # 数据操作
│   │   └── 03-advanced.md         # 高级特性
│   │
│   ├── elasticsearch/             # Elasticsearch
│   │   ├── 01-architecture.md     # 架构与索引
│   │   ├── 02-query-dsl.md        # Query DSL
│   │   ├── 03-aggregations.md     # 聚合分析
│   │   └── 04-integration.md      # 集成指南
│   │
│   ├── hbase/                     # Apache HBase
│   │   ├── 01-architecture.md     # HBase 架构
│   │   ├── 02-data-operations.md  # 数据操作
│   │   └── 03-phoenix.md          # Phoenix 集成
│   │
│   ├── influxdb/                  # InfluxDB
│   │   ├── 01-architecture.md     # 架构与 TSM
│   │   ├── 02-line-protocol.md    # Line Protocol
│   │   └── 03-flux.md             # Flux 查询
│   │
│   └── iotdb/                     # Apache IoTDB
│       ├── 01-architecture.md     # IoTDB 架构
│       ├── 02-session-api.md      # Session API
│       └── 03-write-optimization.md # 写入优化
│
├── 07-data-integration/           # 数据集成
│   ├── flink-cdc/                 # Flink CDC
│   │   ├── 01-architecture.md     # CDC 架构
│   │   ├── 02-mysql-cdc.md        # MySQL CDC
│   │   ├── 03-postgres-cdc.md     # PostgreSQL CDC
│   │   └── 04-pipeline-sync.md    # 管道同步
│   │
│   ├── datax/                     # DataX
│   │   ├── 01-quickstart.md       # 快速开始
│   │   ├── 02-reader-writer.md    # Reader/Writer
│   │   └── 03-job-config.md       # 作业配置
│   │
│   └── seatunnel/                 # SeaTunnel
│       ├── 01-quickstart.md       # 快速开始
│       ├── 02-config-guide.md     # 配置指南
│       └── 03-connector-guide.md  # 连接器指南
```

## 快速导航

### 流处理引擎

| 主题 | 文档 | 说明 |
|------|------|------|
| Flink 入门 | [01-stream-processing/flink/](01-stream-processing/flink/) | DataStream API、状态管理、窗口 |
| Spark Streaming | [01-stream-processing/spark/](01-stream-processing/spark/) | RDD、Structured Streaming |
| Fluss 实时 | [01-stream-processing/fluss/](01-stream-processing/fluss/) | 新一代流引擎 |

### 消息队列

| 主题 | 文档 | 说明 |
|------|------|------|
| Kafka 核心 | [02-message-queue/kafka/](02-message-queue/kafka/) | 生产消费、Streams |
| Pulsar 消息 | [02-message-queue/pulsar/](02-message-queue/pulsar/) | 多租户、Functions |

### OLAP 数据库

| 主题 | 文档 | 说明 |
|------|------|------|
| Doris 实时 | [03-olap/doris/](03-olap/doris/) | 向量化执行、Bitmap |
| ClickHouse | [03-olap/clickhouse/](03-olap/clickhouse/) | MergeTree、物化视图 |
| Druid 实时 | [03-olap/druid/](03-olap/druid/) | 实时摄取 |
| Kylin OLAP | [03-olap/kylin/](03-olap/kylin/) | Cube 构建 |
| Presto/Trino | [03-olap/presto-trino/](03-olap/presto-trino/) | 分布式 SQL |

### 数据湖

| 主题 | 文档 | 说明 |
|------|------|------|
| Paimon 入湖 | [04-data-lake/paimon/](04-data-lake/paimon/) | 流批一体、CDC |
| Hudi 数据湖 | [04-data-lake/hudi/](04-data-lake/hudi/) | CoW/MoR、时间旅行 |
| Iceberg 表格式 | [04-data-lake/iceberg/](04-data-lake/iceberg/) | Schema 演进 |
| Delta Lake | [04-data-lake/delta-lake/](04-data-lake/delta-lake/) | ACID 事务 |

### NoSQL

| 主题 | 文档 | 说明 |
|------|------|------|
| Redis 缓存 | [05-nosql/redis/](05-nosql/redis/) | 数据结构、分布式锁 |
| ES 搜索 | [05-nosql/elasticsearch/](05-nosql/elasticsearch/) | 倒排索引、聚合 |
| HBase | [05-nosql/hbase/](05-nosql/hbase/) | LSM 树、RowKey |
| InfluxDB 时序 | [05-nosql/influxdb/](05-nosql/influxdb/) | TSM、Line Protocol |
| IoTDB 时序 | [05-nosql/iotdb/](05-nosql/iotdb/) | IoT 时序数据 |

### 数据集成

| 主题 | 文档 | 说明 |
|------|------|------|
| Flink CDC | [07-data-integration/flink-cdc/](07-data-integration/flink-cdc/) | 实时 CDC 同步 |
| DataX | [07-data-integration/datax/](07-data-integration/datax/) | 离线数据同步 |
| SeaTunnel | [07-data-integration/seatunnel/](07-data-integration/seatunnel/) | 统一集成 |

## 学习路径建议

### 入门路径

1. **Kafka 基础** -> [02-message-queue/kafka/](02-message-queue/kafka/)
2. **Flink 入门** -> [01-stream-processing/flink/](01-stream-processing/flink/)
3. **Doris/ClickHouse** -> [03-olap/doris/](03-olap/doris/) 或 [03-olap/clickhouse/](03-olap/clickhouse/)

### 进阶路径

1. **数据湖技术** -> [04-data-lake/paimon/](04-data-lake/paimon/)
2. **CDC 实时同步** -> [07-data-integration/flink-cdc/](07-data-integration/flink-cdc/)
3. **AI/LLM 场景** -> [00-overview/ai-llm-scenarios.md](00-overview/ai-llm-scenarios.md)

### 专项深入

- **时序数据**: InfluxDB / IoTDB
- **搜索分析**: Elasticsearch
- **NoSQL 存储**: HBase / Redis
- **批处理**: Spark

## 相关资源

- [项目首页 README](../README.md)
- [技能卡目录](../.claude/skills/)
- [GitHub 项目](https://github.com/tiammomo/Shuai-BigData)
