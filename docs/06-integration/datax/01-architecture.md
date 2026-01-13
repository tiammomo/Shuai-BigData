# DataX 架构详解

## 目录

- [概述](#概述)
- [核心概念](#核心概念)
- [架构原理](#架构原理)
- [Reader 插件](#reader-插件)
- [Writer 插件](#writer-插件)

---

## 概述

DataX 是阿里巴巴开源的离线数据同步工具，支持丰富的数据源。

### 核心特性

| 特性 | 说明 |
|------|------|
| **丰富数据源** | 20+ 数据源 |
| **高效同步** | 多线程并行 |
| **灵活配置** | JSON 作业配置 |
| **断点续传** | 支持增量同步 |
| **类型转换** | 自动类型映射 |

---

## 核心概念

### 核心术语

```
┌─────────────────────────────────────────────────────────────────┐
│                    DataX 核心概念                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Job         │  作业，数据同步任务                              │
│  Task        │  任务，Job 拆分后的执行单元                      │
│  TaskGroup   │  任务组，多个 Task 的集合                       │
│  Channel     │  通道，数据传输通道                             │
│  Reader      │  读取插件，数据源读取                           │
│  Writer      │  写入插件，数据目标写入                         │
│  Transformer │  转换插件，数据转换                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 架构原理

### 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                    DataX 架构                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Job Config                           │   │
│  │  - 源端配置 (Reader)                                    │   │
│  │  - 目标端配置 (Writer)                                  │   │
│  │  - 通道配置 (Channel)                                   │   │
│  │  - 转换配置 (Transformer)                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                    │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Job Container                        │   │
│  │  ┌───────────────────────────────────────────────────┐  │   │
│  │  │  TaskGroup 1  │  TaskGroup 2  │  TaskGroup 3      │  │   │
│  │  │  ┌─────────┐  │  ┌─────────┐  │  ┌─────────┐      │  │   │
│  │  │  │  Task   │  │  │  Task   │  │  │  Task   │      │  │   │
│  │  │  │ Channel │  │  │ Channel │  │  │ Channel │      │  │   │
│  │  │  │ Reader  │──┼──│ Reader  │──┼──│ Reader  │      │  │   │
│  │  │  │ Writer  │◄─┼──│ Writer  │◄─┼──│ Writer  │      │  │   │
│  │  │  └─────────┘  │  └─────────┘  │  └─────────┘      │  │   │
│  │  └───────────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 执行流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    DataX 执行流程                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. 解析 Job 配置                                               │
│  2. 切分 Job 为多个 Task                                        │
│  3. 分配 Task 到 TaskGroup                                      │
│  4. 并行执行 TaskGroup                                          │
│  5. 每个 Task 内 Reader -> Channel -> Writer                   │
│  6. 收集执行结果                                                │
│  7. 生成执行报告                                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Reader 插件

### 支持的 Reader

| 数据源 | 插件名 | 说明 |
|--------|--------|------|
| MySQL | mysqlreader | MySQL 数据库 |
| Oracle | oraclereader | Oracle 数据库 |
| SQLServer | sqlserverreader | SQL Server |
| PostgreSQL | postgresqlreader | PostgreSQL |
| Hive | hdfsreader | HDFS 文件 |
| HDFS | hdfsreader | HDFS 文件 |
| FTP | ftpreader | FTP 文件 |
| OSS | ossreader | 阿里云 OSS |
| MongoDB | mongoreader | MongoDB |

### MySQL Reader 配置

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "password",
            "column": [
              "id",
              "name",
              "age",
              "create_time"
            ],
            "splitPk": "id",
            "where": "create_time >= '2024-01-01'",
            "querySql": "SELECT id, name, age FROM users WHERE status = 1",
            "connection": [
              {
                "table": [
                  "users",
                  "orders"
                ],
                "jdbcUrl": [
                  "jdbc:mysql://host1:3306/db",
                  "jdbc:mysql://host2:3306/db"
                ]
              }
            ]
          }
        }
      }
    ]
  }
}
```

### HDFS Reader 配置

```json
{
  "reader": {
    "name": "hdfsreader",
    "parameter": {
      "defaultFS": "hdfs://namenode:8020",
      "path": "/data/input/*",
      "column": [
        {"index": 0, "type": "long"},
        {"index": 1, "type": "string"},
        {"index": 2, "type": "double"}
      ],
      "fileType": "text",
      "encoding": "UTF-8"
    }
  }
}
```

---

## Writer 插件

### 支持的 Writer

| 数据源 | 插件名 | 说明 |
|--------|--------|------|
| MySQL | mysqlwriter | MySQL 数据库 |
| Oracle | oraclewriter | Oracle 数据库 |
| SQLServer | sqlserverwriter | SQL Server |
| PostgreSQL | postgresqlwriter | PostgreSQL |
| Hive | hdfswriter | HDFS 文件 |
| HDFS | hdfswriter | HDFS 文件 |
| FTP | ftpwriter | FTP 文件 |
| OSS | osswriter | 阿里云 OSS |
| ES | elasticsearchwriter | Elasticsearch |

### MySQL Writer 配置

```json
{
  "writer": {
    "name": "mysqlwriter",
    "parameter": {
      "username": "root",
      "password": "password",
      "column": [
        "id",
        "name",
        "age",
        "create_time"
      ],
      "preSql": "TRUNCATE TABLE target_table",
      "postSql": "SELECT COUNT(*) FROM target_table",
      "batchSize": 1000,
      "connection": [
        {
          "table": ["target_table"],
          "jdbcUrl": "jdbc:mysql://host:3306/db"
        }
      ]
    }
  }
}
```

### HDFS Writer 配置

```json
{
  "writer": {
    "name": "hdfswriter",
    "parameter": {
      "defaultFS": "hdfs://namenode:8020",
      "path": "/data/output",
      "fileName": "data",
      "column": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "amount", "type": "double"}
      ],
      "fileType": "orc",
      "writeMode": "truncate"
    }
  }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [02-job-config.md](02-job-config.md) | 作业配置 |
| [03-best-practices.md](03-best-practices.md) | 最佳实践 |
| [README.md](README.md) | 索引文档 |
