# DataX 中文文档

> DataX 是阿里巴巴开源的离线数据同步工具，支持多种数据源。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |

## 快速入门

```json
// job.json 配置示例
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "username": "root",
            "password": "password",
            "connection": [{
              "querySql": ["SELECT * FROM source_table"],
              "jdbcUrl": ["jdbc:mysql://host:3306/db"]
            }]
          }
        },
        "writer": {
          "name": "hdfswriter",
          "parameter": {
            "defaultFS": "hdfs://namenode:9000",
            "path": "/data/target",
            "fileName": "datax",
            "column": [{"name": "col1", "type": "string"}],
            "writeMode": "truncate"
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 4
      },
      "errorLimit": {
        "percentage": 0.02,
        "record": 100
      }
    }
  }
}
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     DataX 核心概念                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Reader                             │   │
│  │  - 数据读取插件                                          │   │
│  │  - 支持多种数据源                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Writer                              │   │
│  │  - 数据写入插件                                          │   │
│  │  - 支持多种目标                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Channel                            │   │
│  │  - 数据传输通道                                          │   │
│  │  - 并行度控制                                            │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 支持的数据源

| 分类 | 数据源 |
|-----|-------|
| 关系型 | MySQL, Oracle, PostgreSQL, SQL Server |
| NoSQL | MongoDB, Redis, HBase |
| 大数据 | Hive, HDFS, ClickHouse |
| API | HTTP, FTP |

## 运行命令

```bash
# 执行任务
python datax.py job.json

# 指定速度
python datax.py job.json --speed 10

# 指定通道数
python datax.py job.json --thread 4
```

## 相关资源

- [GitHub 仓库](https://github.com/alibaba/DataX)
- [01-architecture.md](01-architecture.md)
