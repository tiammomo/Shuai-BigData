# Shuai-BigData 大数据学习项目

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java Version](https://img.shields.io/badge/Java-1.8+-green.svg)](https://www.oracle.com/java/technologies/jav8-downloads.html)
[![Maven Version](https://img.shields.io/badge/Maven-3.6+-blue.svg)](https://maven.apache.org/)

## 项目简介

Shuai-BigData 是一个专注于 **大数据技术栈实战** 的学习项目，提供主流大数据组件的完整教程和示例代码。通过本项目，您可以学习到从环境搭建到生产实践的完整知识体系。

### 核心特性

- **技术全面**: 涵盖 25+ 个主流大数据组件
- **实战导向**: 每个组件都配有生产级别的示例代码
- **深度文档**: 每篇教程都包含架构原理、最佳实践和 FAQ
- **持续更新**: 定期添加新组件和最佳实践

### 适用人群

- 大数据开发工程师
- 数据平台架构师
- ETL 开发工程师
- 运维工程师
- 大数据专业学生

---

## 学习文档目录 (learn_docs)

本文档体系按照技术领域组织，每个模块包含架构原理、使用指南、最佳实践等内容。

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
├── 06-time-series/                # 时序数据库
│   ├── influxdb/                  # InfluxDB (详见 05-nosql)
│   └── iotdb/                     # IoTDB (详见 05-nosql)
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
│
└── README.md                       # 学习文档索引
```

### 快速导航

| 分类 | 主题 | 文档路径 |
|------|------|----------|
| 流处理 | Flink 入门 | [learn_docs/01-stream-processing/flink/](learn_docs/01-stream-processing/flink/) |
| 流处理 | Spark Streaming | [learn_docs/01-stream-processing/spark/](learn_docs/01-stream-processing/spark/) |
| 消息队列 | Kafka 核心 | [learn_docs/02-message-queue/kafka/](learn_docs/02-message-queue/kafka/) |
| OLAP | Doris 实时分析 | [learn_docs/03-olap/doris/](learn_docs/03-olap/doris/) |
| OLAP | ClickHouse 实战 | [learn_docs/03-olap/clickhouse/](learn_docs/03-olap/clickhouse/) |
| 数据湖 | Paimon 实时入湖 | [learn_docs/04-data-lake/paimon/](learn_docs/04-data-lake/paimon/) |
| 数据湖 | Hudi 数据湖 | [learn_docs/04-data-lake/hudi/](learn_docs/04-data-lake/hudi/) |
| 数据集成 | Flink CDC | [learn_docs/07-data-integration/flink-cdc/](learn_docs/07-data-integration/flink-cdc/) |
| AI 场景 | LLM + 数据湖 | [learn_docs/00-overview/ai-llm-scenarios.md](learn_docs/00-overview/ai-llm-scenarios.md) |

---

## 技术栈概览

### 核心计算引擎

| 组件 | 版本 | 用途 | 关键特性 |
|------|------|------|----------|
| Apache Flink | 1.18.1 | 实时流处理 | 精确一次语义、流批一体 |
| Apache Fluss | 1.0.0 | 实时流处理 | 毫秒延迟、新一代流引擎 |
| Apache Spark | 3.5.3 | 分布式计算 | RDD、Spark SQL、结构化流 |
| Apache Kafka | 3.7.0 | 消息队列 | 高吞吐、持久化、流处理 |
| Apache Pulsar | 3.0.0 | 消息队列 | 多租户、存算分离 |

### OLAP 数据库

| 组件 | 版本 | 用途 | 关键特性 |
|------|------|------|----------|
| Apache Doris | 2.1.0 | 实时分析 | 向量化执行、Bitmap 索引 |
| ClickHouse | 23.8 | 列式存储 | MergeTree、高性能聚合 |
| Apache Druid | 26.0 | 实时分析 | 列式存储、实时摄取 |
| Apache Kylin | 4.0 | OLAP 立方体 | 预计算、多维分析 |
| Presto/Trino | 0.28 | 分布式 SQL | 跨数据源 JOIN |

### 数据湖与表格式

| 组件 | 版本 | 用途 | 关键特性 |
|------|------|------|----------|
| Apache Paimon | 1.1.0 | 流式数据湖 | 实时入湖、CDC 同步 |
| Apache Hudi | 0.14 | 数据湖 | CoW/MoR、时间旅行 |
| Apache Iceberg | 1.5.2 | 表格式 | Schema 演进、Branch |
| Delta Lake | 3.0 | 表格式 | ACID 事务、Merge |

### NoSQL 与时序

| 组件 | 版本 | 用途 | 关键特性 |
|------|------|------|----------|
| Redis | 7.2 | 缓存/NoSQL | 数据结构、分布式锁 |
| Elasticsearch | 8.11 | 搜索/分析 | 倒排索引、聚合 |
| HBase | 2.5.6 | 列式 NoSQL | RowKey、LSM 树 |
| InfluxDB | 2.7 | 时序数据 | TSM 存储、Line Protocol |
| IoTDB | 1.3 | IoT 时序 | 高吞吐、边缘计算 |

### 数据集成

| 组件 | 版本 | 用途 | 关键特性 |
|------|------|------|----------|
| Flink CDC | 3.0 | 实时同步 | CDC 捕获、SQL 集成 |
| DataX | 3.0 | 离线同步 | Reader/Writer 插件 |
| SeaTunnel | 2.3 | 统一集成 | YAML 配置、多引擎 |

---

## 项目结构

```
Shuai-BigData/
├── src/main/java/com/bigdata/example/
│   ├── flink/                    # Flink 示例
│   │   ├── FlinkDataStreamExample.java       # DataStream API 基础
│   │   ├── FlinkStatefulProcessing.java      # 状态管理
│   │   ├── FlinkMultiStreamJoin.java         # 多流处理
│   │   ├── FlinkAdvancedFeatures.java        # 高级特性
│   │   ├── FlinkKafkaExample.java            # Kafka 集成
│   │   ├── FlinkJDBCExample.java             # JDBC 集成
│   │   ├── FlinkTableAPIExample.java         # Table API
│   │   ├── FlinkIcebergExample.java          # Iceberg 集成
│   │   └── FlinkPaimonExample.java           # Paimon 集成
│   │
│   ├── kafka/                    # Kafka 示例
│   │   ├── KafkaExample.java                  # 生产消费
│   │   └── KafkaStreamsExample.java           # Streams API
│   │
│   ├── doris/                    # Doris 示例
│   │   ├── DorisJDBCExample.java              # JDBC 操作
│   │   ├── DorisStreamLoadExample.java        # Stream Load
│   │   ├── DorisFlinkExample.java             # Flink 集成
│   │   ├── DorisSparkExample.java             # Spark 集成
│   │   └── DorisBitmapExample.java            # Bitmap 使用
│   │
│   ├── spark/                    # Spark 示例
│   │   ├── SparkRDDExample.java               # RDD 操作
│   │   ├── SparkSQLExample.java               # Spark SQL
│   │   ├── SparkKafkaExample.java             # Kafka 集成
│   │   ├── SparkHiveExample.java              # Hive 集成
│   │   ├── SparkIcebergExample.java           # Iceberg 集成
│   │   ├── SparkHBaseExample.java             # HBase 集成
│   │   └── SparkPaimonExample.java            # Paimon 集成
│   │
│   ├── redis/                    # Redis 示例
│   │   └── RedisExample.java                  # Jedis 操作
│   │
│   ├── elasticsearch/            # Elasticsearch 示例
│   │   ├── ElasticsearchExample.java          # 基础操作
│   │   ├── SparkElasticsearchExample.java     # Spark 集成
│   │   └── FlinkElasticsearchExample.java     # Flink 集成
│   │
│   ├── hbase/                    # HBase 示例
│   │   ├── HBaseExample.java                  # 基础操作
│   │   └── SparkHBaseExample.java             # Spark 集成
│   │
│   ├── pulsar/                   # Pulsar 示例
│   │   └── PulsarExample.java                 # 生产消费
│   │
│   ├── clickhouse/               # ClickHouse 示例
│   │   └── ClickHouseExample.java             # JDBC 操作
│   │
│   ├── druid/                    # Druid 示例
│   │   └── DruidExample.java                  # 实时摄取
│   │
│   ├── influxdb/                 # InfluxDB 示例
│   │   └── InfluxDBExample.java               # 时序操作
│   │
│   ├── iotdb/                    # IoTDB 示例
│   │   └── IoTDBExample.java                  # IoT 操作
│   │
│   ├── hudi/                     # Hudi 示例
│   │   ├── HudiExample.java                   # 基础操作
│   │   ├── HudiSparkExample.java              # Spark 集成
│   │   └── HudiFlinkExample.java              # Flink 集成
│   │
│   ├── iceberg/                  # Iceberg 示例
│   │   ├── IcebergExample.java                # 基础操作
│   │   ├── IcebergSparkExample.java           # Spark 集成
│   │   └── IcebergFlinkExample.java           # Flink 集成
│   │
│   ├── delta/                    # Delta Lake 示例
│   │   └── DeltaLakeExample.java              # ACID 操作
│   │
│   ├── cdc/                      # CDC 示例
│   │   └── FlinkCDCExample.java               # CDC 同步
│   │
│   ├── seatunnel/                # SeaTunnel 示例
│   │   └── SeaTunnelExample.java              # YAML 配置
│   │
│   ├── presto/                   # Presto/Trino 示例
│   │   └── PrestoExample.java                 # SQL 查询
│   │
│   └── common/                   # 公共工具类
│       ├── ConfigLoader.java                 # 配置加载
│       ├── DateTimeUtils.java                # 日期时间
│       ├── KafkaUtils.java                   # Kafka 工具
│       └── HBaseUtils.java                   # HBase 工具
│
├── learn_docs/                   # 学习文档 (83+ 篇)
│   ├── 00-overview/              # 概览与架构图
│   ├── 01-stream-processing/     # 流处理引擎
│   ├── 02-message-queue/         # 消息队列
│   ├── 03-olap/                  # OLAP 数据库
│   ├── 04-data-lake/             # 数据湖与表格式
│   ├── 05-nosql/                 # NoSQL 数据库
│   ├── 07-data-integration/      # 数据集成
│   └── README.md                 # 文档索引
│
├── scripts/                      # 部署脚本
│   ├── start-flink.sh            # Flink 集群启动
│   ├── start-kafka.sh            # Kafka 集群启动
│   └── start-all.sh              # 一键启动
│
├── config/                       # 配置文件
│   ├── flink-conf.yaml          # Flink 配置
│   ├── log4j2.properties         # 日志配置
│   └── hadoop-conf/             # Hadoop 配置
│
├── .claude/                      # Claude Code 配置
│   ├── CLAUDE.md                 # 项目说明
│   └── skills/                   # 技能卡 (AI 辅助)
│       ├── batch-processing/     # 批处理技能
│       ├── data-integration/     # 数据集成技能
│       ├── data-lake/            # 数据湖技能
│       ├── message-queue/        # 消息队列技能
│       ├── olap/                 # OLAP 技能
│       ├── stream-processing/    # 流处理技能
│       └── common-issues.json    # 常见问题
│
├── pom.xml                       # Maven 配置
└── README.md                     # 项目说明
```

---

## 快速开始

### 环境要求

| 组件 | 要求 | 说明 |
|------|------|------|
| Java | 1.8+ | 推荐 JDK 11/17 |
| Maven | 3.6+ | 构建工具 |
| Scala | 2.12+ | Spark 需要 |
| 内存 | 8GB+ | 开发环境推荐 16GB |
| 磁盘 | 50GB+ | 存储测试数据 |

### 环境安装

```bash
# 1. 安装 Java
# Linux (Ubuntu/Debian)
sudo apt-get install openjdk-11-jdk

# macOS
brew install openjdk@11

# Windows
# 下载安装包: https://adoptium.net/

# 2. 安装 Maven
# Linux/macOS
wget https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz
tar -xzf apache-maven-3.9.6-bin.tar.gz
export PATH=$PATH:/path/to/apache-maven-3.9.6/bin

# 3. 验证安装
java -version
mvn -version
```

### 编译项目

```bash
# 克隆项目
git clone https://github.com/your-repo/Shuai-BigData.git
cd Shuai-BigData

# 编译项目 (跳过测试)
mvn clean compile -DskipTests

# 完整编译 (包括测试)
mvn clean package -DskipTests

# 编译指定模块
mvn clean compile -pl flink -am
```

### 运行示例

```bash
# 运行 Flink 示例
mvn exec:java -Dexec.mainClass="com.bigdata.example.flink.FlinkDataStreamExample"

# 运行 Kafka 示例
mvn exec:java -Dexec.mainClass="com.bigdata.example.kafka.KafkaExample"

# 运行 Doris 示例
mvn exec:java -Dexec.mainClass="com.bigdata.example.doris.DorisJDBCExample"

# 运行 Spark 示例
mvn exec:java -Dexec.mainClass="com.bigdata.example.spark.SparkRDDExample"
```

---

## 教程索引

### 核心计算引擎

| 教程 | 章节数 | 说明 | 难度 |
|------|--------|------|------|
| [Flink 实时计算](learn_docs/01-stream-processing/flink/) | 6章 | DataStream API、状态管理、窗口、水印、容错 | ⭐⭐⭐ |
| [Spark 分布式计算](learn_docs/01-stream-processing/spark/) | 5章 | RDD、DataFrame、Spark SQL、Structured Streaming | ⭐⭐⭐ |
| [Kafka 消息队列](learn_docs/02-message-queue/kafka/) | 6章 | 生产消费、Streams、Admin API、事务 | ⭐⭐ |
| [Pulsar 消息队列](learn_docs/02-message-queue/pulsar/) | 4章 | Producer、Consumer、Subscription、Functions | ⭐⭐ |

### OLAP 数据库

| 教程 | 章节数 | 说明 | 难度 |
|------|--------|------|------|
| [Doris 实时分析](learn_docs/03-olap/doris/) | 5章 | 数据模型、Bitmap、JDBC、Flink/Spark 集成 | ⭐⭐ |
| [ClickHouse OLAP](learn_docs/03-olap/clickhouse/) | 4章 | MergeTree、物化视图、分布式查询 | ⭐⭐ |
| [Druid 实时分析](learn_docs/03-olap/druid/) | 2章 | 实时摄取、Segment、SQL 查询 | ⭐⭐⭐ |
| [Kylin OLAP 引擎](learn_docs/03-olap/kylin/) | 2章 | Cube 构建、星型模型、聚合组 | ⭐⭐⭐ |

### 数据湖表格式

| 教程 | 章节数 | 说明 | 难度 |
|------|--------|------|------|
| [Paimon 实时入湖](learn_docs/04-data-lake/paimon/) | 3章 | 流批一体、CDC 同步、Spark/Flink 集成 | ⭐⭐⭐ |
| [Hudi 数据湖](learn_docs/04-data-lake/hudi/) | 4章 | CoW/MoR表、时间旅行、Flink/Spark 集成 | ⭐⭐⭐ |
| [Iceberg 数据湖](learn_docs/04-data-lake/iceberg/) | 4章 | Schema 演进、时间旅行、Branch/Merge | ⭐⭐⭐ |
| [Delta Lake 数据湖](learn_docs/04-data-lake/delta-lake/) | 4章 | ACID 事务、Merge、Streaming | ⭐⭐ |

### 数据同步与集成

| 教程 | 章节数 | 说明 | 难度 |
|------|--------|------|------|
| [Flink CDC](learn_docs/07-data-integration/flink-cdc/) | 4章 | MySQL/PostgreSQL CDC 实时同步 | ⭐⭐⭐ |
| [DataX 数据同步](learn_docs/07-data-integration/datax/) | 3章 | Reader/Writer 插件、全量/增量同步 | ⭐⭐ |
| [SeaTunnel 集成](learn_docs/07-data-integration/seatunnel/) | 3章 | YAML 配置、Source/Sink/Transform | ⭐⭐ |

### NoSQL 数据库

| 教程 | 章节数 | 说明 | 难度 |
|------|--------|------|------|
| [Redis 缓存](learn_docs/05-nosql/redis/) | 3章 | Jedis、数据结构、分布式锁、限流 | ⭐ |
| [Elasticsearch 搜索](learn_docs/05-nosql/elasticsearch/) | 4章 | 索引、Query DSL、聚合分析 | ⭐⭐ |
| [HBase 数据库](learn_docs/05-nosql/hbase/) | 3章 | 表操作、过滤器、RowKey 设计 | ⭐⭐ |

### 时序数据库

| 教程 | 章节数 | 说明 | 难度 |
|------|--------|------|------|
| [InfluxDB 时序](learn_docs/05-nosql/influxdb/) | 3章 | TSM 存储、Line Protocol、Flux 查询 | ⭐⭐ |
| [IoTDB 时序](learn_docs/05-nosql/iotdb/) | 3章 | Session API、Tablet、Schema 管理 | ⭐⭐ |

---

## 架构设计

### 数据流架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           数据处理流水线                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │   采集   │ -> │   消息   │ -> │   计算   │ -> │   存储   │         │
│  │  Source  │    │   Queue  │    │  Engine  │    │   Sink   │         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│       │                │                │               │              │
│       v                v                v               v              │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Flume   │    │  Kafka   │    │  Flink   │    │   HDFS   │         │
│  │  CDC     │    │ Pulsar   │    │  Spark   │    │   Doris  │         │
│  │  Logstash│    │          │    │          │    │  HBase   │         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                        数据仓库/湖                               │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │   │
│  │  │  Hive   │  │  Paimon │  │Iceberg  │  │Delta    │            │   │
│  │  │         │  │  Hudi   │  │         │  │Lake     │            │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                       OLAP 查询引擎                              │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │   │
│  │  │  Doris  │  │ClickHouse│ │  Druid  │  │ Presto  │            │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│─────────────────────────────────────────────────────────────────────────┘
```

### 技术选型指南

```
场景                           推荐技术栈
─────────────────────────────────────────────────────────────────
实时流处理                      Flink + Kafka
离线批处理                      Spark + Hive
实时数仓                        Doris / ClickHouse
数据湖建设                      Paimon / Hudi / Iceberg
CDC 实时同步                    Flink CDC
日志收集                        Filebeat + Kafka + ES
时序数据                        InfluxDB / IoTDB
缓存加速                        Redis Cluster
搜索服务                        Elasticsearch
跨数据源查询                    Presto / Trino
AI/LLM 数据管道                 Paimon + Flink CDC + 向量引擎
```

---

## AI/LLM 场景指南

数据湖仓技术与 AI/LLM 的结合是当前热门方向，详见:

- [AI/LLM 数据湖场景](learn_docs/00-overview/ai-llm-scenarios.md)
  - RAG 知识库架构
  - 实时特征工程
  - 模型训练数据管理
  - 向量检索集成

---

## 常见问题

### Q1: 项目编译失败?

```bash
# 1. 检查 Java 版本
java -version  # 需要 1.8+

# 2. 检查 Maven 设置
mvn -version  # 需要 3.6+

# 3. 清理并重新编译
mvn clean
mvn compile -U  # 强制更新依赖

# 4. 检查网络代理
mvn settings.xml
```

### Q2: Flink 本地运行报错?

```java
// 解决: 添加 flink-runtime-web 依赖
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-runtime-web</artifactId>
    <version>${flink.version}</version>
</dependency>
```

### Q3: Kafka 连接失败?

```bash
# 1. 检查 Kafka 服务
telnet localhost 9092

# 2. 检查防火墙
sudo ufw status

# 3. 检查主机名配置
cat /etc/hosts
# 确保包含: 127.0.0.1 localhost
```

---

## 贡献指南

欢迎贡献代码和文档！

### 添加新教程

1. 在 `learn_docs/` 对应分类下创建章节文档
2. 遵循现有教程的格式和结构
3. 添加代码示例和详细注释
4. 更新 `README.md` 的教程索引

### 代码规范

- 使用 Lombok 简化 POJO 类
- 添加适当的日志记录
- 编写单元测试
- 使用 Checkstyle 检查代码格式

---

## 参考资源

### 官方文档

- [Apache Flink](https://flink.apache.org/)
- [Apache Spark](https://spark.apache.org/)
- [Apache Kafka](https://kafka.apache.org/)
- [Apache Doris](https://doris.apache.org/)
- [ClickHouse](https://clickhouse.com/)
- [Apache Paimon](https://paimon.apache.org/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Hudi](https://hudi.apache.org/)

### 中文文档

- [Flink 中文文档](https://flink.apache.org/zh/)
- [Spark 中文文档](https://spark.apache.org/docs/zh/)
- [Kafka 中文文档](https://kafka.apache.org/documentation/)

---

## 许可证

本项目采用 Apache License 2.0 许可证。

---

## 更新日志

### v1.0.0 (2025-01)

- 初始版本发布
- 包含 21 个教程文档
- 包含 50+ 示例代码
- 覆盖主流大数据组件

### v1.1.0 (2025-01)

- 拆分并重组所有 TUTORIAL 文件到子文件夹章节文档
- 新增 Pulsar/Elasticsearch/HBase/时序数据库等组件详细操作文档
- 新增 DataX/Flink-CDC/SeaTunnel 配置指南
- 新增 Druid/Kylin/Presto-Trino 使用指南
- 删除原有的 21 个 TUTORIAL.md 文件
- 文档总数达到 83+ 个文件

### v1.2.0 (2026-01)

- 重命名 docs 为 learn_docs，优化文档组织结构
- 新增 AI/LLM 数据湖场景文档
- 新增数据湖 skills: Paimon、Iceberg、Hudi、Delta Lake
- 新增 Flink CDC skills
- 更新完善 Doris、ClickHouse、Flink、Kafka skills
- 文档与 skills 形成完整知识体系
