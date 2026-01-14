# SeaTunnel 中文文档

> SeaTunnel 是一个高性能的分布式数据集成平台。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |

## 快速入门

```hocon
// config.v2.conf
env {
  job.mode = "batch"
  parallelism = 4
}

source {
  MySQL-CDC {
    url = "jdbc:mysql://localhost:3306/db"
    username = "root"
    password = "password"
    table-names = ["db.table1", "db.table2"]
  }
}

transform {
  # 过滤器配置
}

sink {
  ClickHouse {
    host = "localhost:8123"
    database = "db"
    table = "output_table"
  }
}
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     SeaTunnel 核心架构                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Source                              │   │
│  │  - 数据输入插件                                          │   │
│  │  - 多种数据源支持                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Transform                           │   │
│  │  - 数据转换处理                                          │   │
│  │  - 过滤、映射、聚合                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Sink                                │   │
│  │  - 数据输出插件                                          │   │
│  │  - 多种目标支持                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Engine                              │   │
│  │  - Flink / Spark 引擎                                    │   │
│  │  - Zeta 引擎 (原生)                                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 运行命令

```bash
# Flink 引擎
./bin/seatunnel-flink-connector-v2-start \
    --config config.v2.conf

# Spark 引擎
./bin/seatunnel-spark-connector-v2-start \
    --master yarn \
    --deploy-mode cluster \
    --config config.v2.conf

# Zeta 引擎 (原生)
./bin/seatunnel.sh --config config.v2.conf
```

## 支持的数据源

| 分类 | 数据源 |
|-----|-------|
| CDC | MySQL, PostgreSQL, MongoDB, Oracle |
| 数据库 | MySQL, PostgreSQL, Oracle, SQL Server |
| 大数据 | Hive, HDFS, ClickHouse, Doris |
| 云服务 | S3, GCS, OSS, Kafka |

## 相关资源

- [官方文档](https://seatunnel.apache.org/learn_docs/)
- [01-architecture.md](01-architecture.md)
