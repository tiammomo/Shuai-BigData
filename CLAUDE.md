# CLAUDE.md

This file provides guidance for Claude Code when working with this codebase.

## Git Commit Rules

### Behavior
When generating git commit messages:
- **Strictly Forbidden**: Never include text indicating the message was AI-generated (e.g., "Written by Claude", "AI-generated").
- **No Footers**: Do not append "Signed-off-by" or "Co-authored-by" lines unless explicitly told to do so for a specific human user.
- **Direct Output**: Output the commit message immediately without introductory text (e.g., skip "Sure, here is the commit...").

### Format Standard
- Use the Conventional Commits format: `<type>(<scope>): <subject>`
- Allowed types: feat, fix, docs, style, refactor, perf, test, build, ci, chore, revert.
- Keep the first line under 72 characters.

## Project Context

Shuai-BigData is a comprehensive big data technology learning project covering 21+ major big data components.

### Key Technologies
- **Stream Processing**: Apache Flink, Apache Spark Streaming
- **Message Queue**: Apache Kafka, Apache Pulsar
- **OLAP**: Apache Doris, ClickHouse, Druid, Kylin, Presto/Trino
- **Time Series**: InfluxDB, IoTDB
- **NoSQL**: HBase, Redis, Elasticsearch
- **Data Lake**: Apache Hudi, Apache Iceberg, Delta Lake
- **Data Integration**: Flink CDC, DataX, SeaTunnel

### Documentation Structure
```
docs/
├── 00-overview/              # Overview & architecture
├── 01-stream-processing/     # Flink, Spark
├── 02-message-queue/         # Kafka, Pulsar
├── 03-olap/                  # Doris, ClickHouse, Druid, Kylin, Presto-Trino
├── 04-data-lake/             # Hudi, Iceberg, Delta Lake
├── 05-nosql/                 # Redis, ES, HBase, InfluxDB, IoTDB
└── 06-integration/           # DataX, Flink-CDC, SeaTunnel
```

### Build & Test
```bash
# Maven project
mvn clean compile -DskipTests  # Compile
mvn clean package -DskipTests  # Build package
```

### Documentation Guidelines
- Follow existing documentation format in each component folder
- Each component should have: README.md + chapter files (01-xxx.md, 02-xxx.md, etc.)
- Use Chinese for documentation content (project language)
- Include code examples with comments
