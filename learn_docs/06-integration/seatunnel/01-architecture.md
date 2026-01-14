# SeaTunnel 架构详解

## 目录

- [概述](#概述)
- [核心概念](#核心概念)
- [架构原理](#架构原理)
- [连接器](#连接器)
- [转换器](#转换器)

---

## 概述

SeaTunnel 是一个高性能的分布式数据集成平台。

### 核心特性

| 特性 | 说明 |
|------|------|
| **高性能** | 分布式并行处理 |
| **易用性** | 简洁的配置语法 |
| **丰富连接** | 40+ 数据源 |
| **多引擎** | Spark / Flink / SeaTunnel Engine |
| **CDC 支持** | 实时数据捕获 |

---

## 核心概念

### 核心术语

```
┌─────────────────────────────────────────────────────────────────┐
│                    SeaTunnel 核心概念                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Source     │  数据来源                                        │
│  Sink       │  数据目标                                        │
│  Transform  │  数据转换                                        │
│  Env        │  运行环境配置                                    │
│  Config     │  作业配置文件                                    │
│  Engine     │  执行引擎 (Spark/Flink/Seatunnel)                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 架构原理

### 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                    SeaTunnel 架构                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Config (配置文件)                     │   │
│  │  env { ... }                                            │   │
│  │  source { ... }                                         │   │
│  │  transform { ... }                                      │   │
│  │  sink { ... }                                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                SeaTunnel Engine                          │   │
│  │  ┌───────────────────────────────────────────────────┐  │   │
│  │  │  Source(1)  →  Transform(1)  →  Sink(1)           │  │   │
│  │  │  Source(2)  →  Transform(2)  →  Sink(2)           │  │   │
│  │  │  Source(3)  →  Transform(3)  →  Sink(3)           │  │   │
│  │  └───────────────────────────────────────────────────┘  │   │
│  │  - 任务调度                                             │   │
│  │  - 资源管理                                             │   │
│  │  - 故障恢复                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Spark / Flink / Seatunnel Engine           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 连接器

### Source 连接器

| 类型 | 连接器 | 说明 |
|------|--------|------|
| 数据库 | MySQL, PostgreSQL, Oracle, SQLServer | CDC 读取 |
| 文件 | HDFS, S3, OSS, FTP | 文件读取 |
| 消息队列 | Kafka, Pulsar, RocketMQ | 消息读取 |
| ES | Elasticsearch | 文档读取 |

### Sink 连接器

| 类型 | 连接器 | 说明 |
|------|--------|------|
| 数据库 | MySQL, PostgreSQL, ClickHouse | 数据写入 |
| 文件 | HDFS, S3, OSS | 文件写入 |
| 消息队列 | Kafka, Pulsar | 消息写入 |
| ES | Elasticsearch | 文档写入 |

---

## 配置示例

### MySQL to Kafka

```hocon
env {
  job.name = "mysql-to-kafka"
  parallelism = 4
}

source {
  MySQL-CDC {
    url = "jdbc:mysql://localhost:3306/mydb"
    username = "root"
    password = "password"
    table-names = ["mydb.orders", "mydb.users"]
    base-url = "jdbc:mysql://localhost:3306/mydb"
  }
}

transform {
  # 可选的数据转换
  # sql {
  #   sql = "SELECT * FROM orders WHERE status = 'completed'"
  # }
}

sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "output-topic"
    format = "json"
  }
}
```

### Kafka to MySQL

```hocon
env {
  job.name = "kafka-to-mysql"
  parallelism = 2
}

source {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topics = "input-topic"
    format = "json"
    consumer.group = "seatunnel-group"
  }
}

transform {
  sql {
    sql = "SELECT id, name, amount * 1.1 AS amount FROM orders"
  }
}

sink {
  JDBC {
    url = "jdbc:mysql://localhost:3306/mydb"
    username = "root"
    password = "password"
    table = "target_table"
    save_mode = "update"
  }
}
```

### HDFS to Elasticsearch

```hocon
env {
  job.name = "hdfs-to-es"
  parallelism = 4
}

source {
  HdfsFile {
    path = "/data/input"
    file_format_type = "parquet"
  }
}

transform {
  # 字段映射和转换
  FieldMapper {
    source_field = "user_name"
    result_field = "username"
  }
}

sink {
  Elasticsearch {
    hosts = ["localhost:9200"]
    index = "my_index"
    index_type = "_doc"
  }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [02-job-config.md](02-job-config.md) | 作业配置 |
| [03-connector-guide.md](03-connector-guide.md) | 连接器指南 |
| [04-best-practices.md](04-best-practices.md) | 最佳实践 |
| [README.md](README.md) | 索引文档 |
