# AI/LLM 与数据湖仓集成场景指南

## 目录

- [概述](#概述)
- [数据湖仓在 AI/LLM 中的角色](#数据湖仓在-aillm-中的角色)
- [核心场景](#核心场景)
  - [RAG 知识库](#rag-知识库)
  - [数据预处理与特征工程](#数据预处理与特征工程)
  - [模型训练数据管理](#模型训练数据管理)
  - [向量检索与语义搜索](#向量检索与语义搜索)
- [技术架构](#技术架构)
- [最佳实践](#最佳实践)
- [相关文档](#相关文档)

---

## 概述

大语言模型（LLM）与数据湖仓的结合正在重塑企业数据基础设施。数据湖仓提供统一的数据存储层，支撑从数据采集、特征工程到模型推理的全流程，是 AI 应用落地的关键基础设施。

**核心价值**：
- 统一存储多模态数据（文本、图像、结构化数据）
- 支持数据血缘追踪与版本管理
- 提供实时数据流处理能力
- 实现数据治理与安全管控

---

## 数据湖仓在 AI/LLM 中的角色

### 数据存储层

```
┌─────────────────────────────────────────────────────────────┐
│                      AI/LLM 应用层                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ RAG 系统  │  │ 特征存储  │  │ 模型服务  │  │ 数据标注  │    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │
└───────┼─────────────┼─────────────┼─────────────┼──────────┘
        │             │             │             │
        ▼             ▼             ▼             ▼
┌─────────────────────────────────────────────────────────────┐
│                    数据处理与特征层                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │ 数据清洗  │  │ 特征计算  │  │ 向量化   │  │ 数据验证  │    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │
└───────┼─────────────┼─────────────┼─────────────┼──────────┘
        │             │             │             │
        ▼             ▼             ▼             ▼
┌─────────────────────────────────────────────────────────────┐
│                    数据湖仓存储层                            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  Apache Paimon / Iceberg / Hudi / Delta Lake         │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │   │
│  │  │ 原始数据  │ │ 特征数据  │ │ 向量索引  │ │ 模型产物  │ │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 数据湖仓特性适配 AI 场景

| 特性 | AI 场景价值 | 适用组件 |
|------|------------|----------|
| 时间旅行 | 模型训练数据版本回溯 | Iceberg, Paimon |
| Schema 演进 | 动态添加特征字段 | 所有组件 |
| 流批一体 | 实时特征更新 + 批量训练 | Paimon, Hudi |
| CDC 接入 | 业务数据实时同步 | Paimon, Flink CDC |
| 分区裁剪 | 高效过滤训练数据子集 | 所有组件 |

---

## 核心场景

### RAG 知识库

#### 架构设计

```
                    ┌─────────────────────────────────────┐
                    │          RAG 系统架构               │
                    └─────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐         ┌───────────────┐         ┌───────────────┐
│  数据采集层    │         │  数据处理层   │         │  服务层       │
│  ┌─────────┐  │         │  ┌─────────┐  │         │  ┌─────────┐  │
│  │ 数据库   │──┼────────▶│  │ 文档切分  │──┼────────▶│  │ 向量检索 │  │
│  │ 文件系统  │  │         │  │ 向量化   │  │         │  │ LLM 调用 │  │
│  │ API     │  │         │  │ 元数据   │  │         │  │ 上下文   │  │
│  └─────────┘  │         │  └─────────┘  │         │  └─────────┘  │
└───────────────┘         └───────────────┘         └───────────────┘
        │                           │                           │
        ▼                           ▼                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Paimon / Iceberg                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ 原始文档库   │  │ 切片索引表   │  │ 元数据表    │              │
│  │ (Append)   │  │ (主键+向量) │  │ (Tag管理)   │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
```

#### Paimon 表设计

```sql
-- 原始文档存储表
CREATE TABLE document_raw (
    doc_id BIGINT,
    title STRING,
    content STRING,
    source_type STRING,          -- 'database', 'filesystem', 'api'
    source_path STRING,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (doc_id)
) WITH (
    'bucket' = '4',
    'merge-engine' = 'deduplicate'
);

-- 文档切片与向量索引表
CREATE TABLE document_chunks (
    chunk_id BIGINT,
    doc_id BIGINT,
    chunk_seq INT,
    content STRING,
    embedding ARRAY<FLOAT>,      -- 向量字段
    metadata STRING,             -- JSON 元数据
    PRIMARY KEY (chunk_id)
) WITH (
    'bucket' = '16',
    'bucket-key' = 'doc_id',
    'merge-engine' = 'deduplicate'
);

-- 标签管理（版本快照）
CALL paimon.system.create_tag('document_chunks', 'v1.0', 123456789);
```

#### Flink CDC 实时同步

```java
// MySQL → Paimon 实时同步
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);
env.enableCheckpointing(60000);

TableEnvironment tableEnv = TableEnvironment.create(env);

// MySQL CDC 源
tableEnv.executeSql(
    "CREATE TABLE mysql_source (" +
    "  id BIGINT," +
    "  title STRING," +
    "  content STRING," +
    "  updated_at TIMESTAMP(3)," +
    "  PRIMARY KEY (id) NOT ENFORCED" +
    ") WITH (" +
    "  'connector' = 'mysql-cdc'," +
    "  'hostname' = 'localhost'," +
    "  'port' = '3306'," +
    "  'username' = 'root'," +
    "  'password' = 'password'," +
    "  'database-name' = 'kb_db'," +
    "  'table-name' = 'documents'" +
    ")"
);

// Paimon 目标
tableEnv.executeSql(
    "CREATE TABLE paimon_sink (" +
    "  doc_id BIGINT," +
    "  title STRING," +
    "  content STRING," +
    "  updated_at TIMESTAMP(3)," +
    "  PRIMARY KEY (doc_id)" +
    ") WITH (" +
    "  'connector' = 'paimon'," +
    "  'path' = 'hdfs://namenode:9000/paimon/warehouse/kb'," +
    "  'sink.parallelism' = '4'" +
    ")"
);

// 同步并触发向量化
tableEnv.executeSql(
    "INSERT INTO paimon_sink " +
    "SELECT * FROM mysql_source"
);
```

### 数据预处理与特征工程

#### 流式特征计算

```python
# PySpark 结构化流式处理
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors

# 实时特征计算流
feature_stream = spark.readStream \
    .format("paimon") \
    .option("path", "hdfs://namenode:9000/paimon/warehouse/events") \
    .load()

# 特征计算
features = feature_stream \
    .groupBy(window("event_time", "1 hour"), "user_id") \
    .agg(
        count("*").alias("event_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        collect_list("event_type").alias("event_types")
    ) \
    .select("user_id", "window.*", "event_count", "total_amount", "avg_amount", "event_types")

# 写入特征存储
features.writeStream \
    .format("paimon") \
    .option("path", "hdfs://namenode:9000/paimon/warehouse/features") \
    .option("checkpointLocation", "/tmp/checkpoint/features") \
    .outputMode("update") \
    .start()
```

#### 特征存储表设计

```sql
-- 实时特征表 (Paimon)
CREATE TABLE feature_realtime (
    feature_key STRING,           -- user_id:item_id
    feature_name STRING,
    feature_value DOUBLE,
    timestamp TIMESTAMP(3),
    PRIMARY KEY (feature_key, feature_name)
) WITH (
    'bucket' = '64',
    'merge-engine' = 'aggregation',
    'fields.feature-value.aggregate-function' = 'last'
);

-- 离线特征表
CREATE TABLE feature_offline (
    feature_key STRING,
    feature_name STRING,
    feature_value DOUBLE,
    dt STRING,
    hr STRING,
    PRIMARY KEY (feature_key, feature_name, dt, hr)
) PARTITIONED BY (dt, hr)
WITH (
    'bucket' = '32',
    'merge-engine' = 'deduplicate'
);
```

### 模型训练数据管理

#### 训练数据集版本化

```sql
-- 创建训练数据集快照
CALL paimon.system.create_tag('training_data', 'v1.0', 123456789);

-- 按时间查询历史数据
SELECT * FROM training_data VERSION AS OF 123456789;

-- 对比不同版本
SELECT
    'v1.0' AS version,
    COUNT(*) AS record_count,
    SUM(label) AS positive_count
FROM training_data VERSION AS OF 123456789
UNION ALL
SELECT
    'v2.0' AS version,
    COUNT(*) AS record_count,
    SUM(label) AS positive_count
FROM training_data VERSION AS OF 123456790;
```

#### 数据质量检查

```sql
-- 数据质量监控
CREATE TABLE data_quality_metrics (
    table_name STRING,
    snapshot_id BIGINT,
    record_count BIGINT,
    null_count BIGINT,
    duplicate_count BIGINT,
    check_time TIMESTAMP(3),
    PRIMARY KEY (table_name, snapshot_id)
);

-- 定期执行质量检查
INSERT INTO data_quality_metrics
SELECT
    'training_data',
    snapshot_id,
    COUNT(*) AS record_count,
    SUM(CASE WHEN label IS NULL THEN 1 ELSE 0 END) AS null_count,
    COUNT(DISTINCT id) AS unique_count
FROM training_data$snapshots s
JOIN training_data t ON s.snapshot_id = t._SNAPSHOT_ID
GROUP BY snapshot_id;
```

### 向量检索与语义搜索

#### 向量索引架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    向量检索架构                                  │
└─────────────────────────────────────────────────────────────────┘

  用户查询 ──────────────────────────────────────────────────┐
       │                                                     │
       ▼                                                     │
┌───────────────┐                                           │
│  查询预处理   │                                           │
│  ┌─────────┐  │                                           │
│  │ 文本切分 │                                           │
│  │ 向量化  │                                           │
│  └─────────┘  │                                           │
└───────┬───────┘                                           │
        │                                                     │
        ▼                                                     │
┌───────────────┐                                           │
│  向量检索引擎  │◀──────────────────────────────────────────┤
│  ┌─────────┐  │    近似最近邻 (ANN) 检索                   │
│  │ Milvus  │  │                                           │
│  │ Pinecone│  │    Top-K 结果返回                          │
│  │ FAISS   │  │                                           │
│  └─────────┘  │                                           │
└───────┬───────┘                                           │
        │                                                     │
        ▼                                                     │
┌───────────────┐                                           │
│  结果后处理   │                                           │
│  ┌─────────┐  │                                           │
│  │ 重排序  │                                           │
│  │ 元数据  │                                           │
│  │ 过滤    │                                           │
│  └─────────┘  │                                           │
└───────┬───────┘                                           │
        │                                                     │
        ▼                                                     │
┌───────────────┐                                           │
│  Paimon 关联  │◀──────────────────────────────────────────┤
│  ┌─────────┐  │    获取完整文档信息                        │
│  │ 元数据表 │                                           │
│  │ 原始数据 │                                           │
│  └─────────┘  │                                           │
└───────────────┘                                           │
```

#### Paimon 向量检索集成

```java
// 向量检索服务
public class VectorSearchService {

    private final PaimonTable paimonTable;
    private final VectorStore vectorStore;

    // 搜索并关联元数据
    public List<SearchResult> search(String query, int topK) {
        // 1. 向量化查询
        float[] queryVector = embedQuery(query);

        // 2. ANN 检索
        List<Long> chunkIds = vectorStore.search(queryVector, topK);

        // 3. 从 Paimon 获取完整信息
        TableScan scan = paimonTable.newScan();
        List<BinaryRow> rows = chunkIds.stream()
            .map(id -> BinaryRowUtil.binaryRow(1))
            .collect(Collectors.toList());

        RecordReader<GenericRow> reader = paimonTable.newRead()
            .createReader(rows, Collections.singletonList("content", "metadata"));

        // 4. 组合结果
        return chunkIds.stream()
            .map(id -> {
                GenericRow row = reader.read(id);
                return new SearchResult(
                    id,
                    row.getString(0).toString(),  // content
                    row.getString(1).toString()   // metadata
                );
            })
            .collect(Collectors.toList());
    }
}
```

---

## 技术架构

### 实时数据管道

```
Kafka/Pulsar ──▶ Flink ──▶ Paimon ──▶ 特征存储/向量引擎
     │              │           │
     │              │           │
     ▼              ▼           ▼
┌─────────┐   ┌─────────┐  ┌─────────┐
│ CDC 同步 │   │ 实时计算 │  │ 快照管理 │
└─────────┘   └─────────┘  └─────────┘

                    │
                    ▼
              ┌─────────────┐
              │  模型推理   │
              │  (实时特征) │
              └─────────────┘
```

### 批处理训练管道

```
Paimon/Iceberg ──▶ Spark ──▶ 训练数据 ──▶ 模型训练
      │                │            │
      ▼                ▼            ▼
┌──────────┐   ┌──────────┐  ┌──────────┐
│ 版本管理  │   │ 数据采样  │  │ 特征工程  │
└──────────┘   └──────────┘  └──────────┘
```

---

## 最佳实践

### 数据布局优化

```sql
-- 按时间分区 + 向量分桶
CREATE TABLE document_vectors (
    chunk_id BIGINT,
    doc_id BIGINT,
    embedding ARRAY<FLOAT>,
    created_date STRING,
    PRIMARY KEY (chunk_id)
) PARTITIONED BY (created_date)
WITH (
    'bucket' = '64',
    'bucket-key' = 'doc_id',
    'merge-engine' = 'deduplicate'
);
```

### 性能优化配置

```sql
-- 向量化读取优化
ALTER TABLE document_vectors
SET TBLPROPERTIES (
    'read.vectorized.enabled' = 'true',
    'read.batch-size' = '4096'
);

-- Compaction 策略
ALTER TABLE document_vectors
SET TBLPROPERTIES (
    'compaction.min.file-num' = '5',
    'compaction.max.file-num' = '50'
);
```

### TTL 与数据清理

```sql
-- 设置数据保留策略
ALTER TABLE document_raw
SET TBLPROPERTIES (
    'table.exec.state.ttl' = '30d',
    'sink.delete-mode' = 'file'
);

-- 手动清理过期数据
CALL paimon.system.expire_snapshots(
    'document_raw',
    30,    -- 保留30天
    10     -- 最少保留10个快照
);
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](../04-data-lake/paimon/01-architecture.md) | Paimon 架构详解 |
| [02-usage.md](../04-data-lake/paimon/02-usage.md) | Paimon 使用指南 |
| [03-spark-sql.md](../04-data-lake/paimon/03-spark-sql.md) | Spark SQL 操作 |
| [Flink CDC](../03-data-integration/flink-cdc/01-usage.md) | CDC 实时同步 |
| [03-flink-integration.md](../04-data-lake/iceberg/03-flink-integration.md) | Flink 集成 |
| [04-spark-sql.md](../04-data-lake/iceberg/04-spark-sql.md) | Spark SQL 操作 |
