# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [1.0.0] - 2024-01-11

### Added

#### Documentation

- **流处理文档**
  - [Flink 完整文档集](docs/01-stream-processing/flink/)
    - 架构详解 (01-architecture.md)
    - DataStream API (02-datastream.md)
    - Table/SQL API (03-table-sql.md)
    - 状态管理 (04-state-checkpoint.md)
    - CEP 复杂事件处理 (05-cep.md)
    - 运维指南 (06-operations.md)

  - [Spark Streaming 文档](docs/01-stream-processing/spark/)
    - 架构详解 (01-architecture.md)
    - Spark SQL (02-spark-sql.md)
    - Streaming 详解 (03-spark-streaming.md)
    - MLlib 机器学习 (04-mllib.md)
    - 性能优化 (05-optimization.md)

- **消息队列文档**
  - [Kafka 完整文档集](docs/02-message-queue/kafka/)
    - 架构详解 (01-architecture.md)
    - 生产者指南 (02-producer.md)
    - 消费者指南 (03-consumer.md)
    - Kafka Streams (04-streams.md)
    - 运维指南 (05-operations.md)
    - 故障排查 (06-troubleshooting.md)

- **OLAP 分析文档**
  - [ClickHouse 文档集](docs/03-olap/clickhouse/)
    - 架构详解 (01-architecture.md)
    - MergeTree 引擎 (02-merge-tree.md)
    - 数据操作 (03-data-operations.md)
    - 性能优化 (04-optimization.md)

  - [Doris 文档集](docs/03-olap/doris/)
    - 架构详解 (01-architecture.md)
    - 数据模型 (02-data-model.md)
    - 数据操作 (03-data-operations.md)
    - SQL 参考 (04-sql-reference.md)
    - 运维指南 (05-operations.md)

- **空目录索引文档**
  - pulsar/README.md
  - druid/README.md
  - kylin/README.md
  - presto-trino/README.md
  - delta-lake/README.md
  - hudi/README.md
  - iceberg/README.md
  - elasticsearch/README.md
  - hbase/README.md
  - influxdb/README.md
  - iotdb/README.md
  - redis/README.md
  - datax/README.md
  - flink-cdc/README.md
  - seatunnel/README.md

#### Scripts & Configurations

- **启动脚本**
  - [scripts/start-kafka.sh](scripts/start-kafka.sh) - Kafka 集群启动脚本
  - [scripts/start-flink.sh](scripts/start-flink.sh) - Flink 集群启动脚本
  - [scripts/start-doris.sh](scripts/start-doris.sh) - Doris 集群启动脚本
  - [scripts/start-clickhouse.sh](scripts/start-clickhouse.sh) - ClickHouse 启动脚本
  - [scripts/start-all.sh](scripts/start-all.sh) - 一键启动所有组件

- **配置文件**
  - [config/flink-conf.yaml](config/flink-conf.yaml) - Flink 配置模板
  - [config/log4j2.properties](config/log4j2.properties) - Log4j2 日志配置

- **Docker 环境**
  - [docker-compose.yml](docker-compose.yml) - 完整大数据组件编排
  - [docker-compose.light.yml](docker-compose.light.yml) - 轻量级组件编排

#### Claude Skills

- [skills.md](.claude/skills.md) - 大数据技能主索引
- [skills/stream-processing/](.claude/skills/stream-processing/) - 流处理技能
- [skills/message-queue/](.claude/skills/message-queue/) - 消息队列技能
- [skills/olap/](.claude/skills/olap/) - OLAP 技能
- [skills/batch-processing/](.claude/skills/batch-processing/) - 批处理技能
- [skills/data-integration/](.claude/skills/data-integration/) - 数据集成技能
- [skills/common-issues.json](.claude/skills/common-issues.json) - 常见问题

#### Unit Tests

- [src/test/java/](src/test/java/)
  - FlinkExampleTest.java - Flink 单元测试
  - SparkExampleTest.java - Spark 单元测试
  - KafkaExampleTest.java - Kafka 单元测试
  - DatabaseExampleTest.java - 数据库单元测试

### Changed

### Deprecated

### Removed

### Fixed

### Security

## Project Structure

```
Shuai-BigData/
├── docs/                          # 文档目录
│   ├── 01-stream-processing/      # 流处理
│   │   ├── flink/                 # Flink 文档
│   │   └── spark/                 # Spark 文档
│   ├── 02-message-queue/          # 消息队列
│   │   └── kafka/                 # Kafka 文档
│   ├── 03-olap/                   # OLAP 分析
│   │   ├── clickhouse/            # ClickHouse 文档
│   │   └── doris/                 # Doris 文档
│   ├── 04-data-lake/              # 数据湖
│   ├── 05-nosql/                  # NoSQL 数据库
│   └── 06-integration/            # 数据集成
├── src/main/java/                 # 源代码
├── src/test/java/                 # 测试代码
├── scripts/                       # 启动脚本
├── config/                        # 配置文件
├── docker-compose.yml             # Docker 编排
├── pom.xml                        # Maven 配置
├── CHANGELOG.md                   # 更新日志
└── README.md                      # 项目说明
```

## Version History

- [1.0.0] - Initial release with comprehensive documentation and examples
