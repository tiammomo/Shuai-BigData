# DataX 配置指南

## 目录

- [Reader 配置](#reader-配置)
- [Writer 配置](#writer-配置)
- [Job 配置](#job-配置)
- [高级特性](#高级特性)

---

## Reader 配置

### MySQL Reader

```json
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
      "where": "status = 'active'",
      "querySql": [
        "SELECT id, name, age, create_time FROM users WHERE create_time > '2024-01-01'"
      ],
      "connection": [
        {
          "jdbcUrl": [
            "jdbc:mysql://localhost:3306/mydb",
            "jdbc:mysql://localhost:3307/mydb"
          ],
          "table": [
            "users"
          ]
        }
      ]
    }
  }
}
```

### PostgreSQL Reader

```json
{
  "reader": {
    "name": "postgresqlreader",
    "parameter": {
      "username": "postgres",
      "password": "password",
      "column": ["*"],
      "connection": [
        {
          "jdbcUrl": ["jdbc:postgresql://localhost:5432/mydb"],
          "schema": "public",
          "table": ["orders"]
        }
      ],
      "fetchSize": 1000
    }
  }
}
```

### HDFS Reader

```json
{
  "reader": {
    "name": "hdfsreader",
    "parameter": {
      "defaultFS": "hdfs://namenode:9000",
      "path": "/data/input",
      "column": [
        {
          "name": "id",
          "type": "long"
        },
        {
          "name": "name",
          "type": "string"
        }
      ],
      "fileType": "text",
      "encoding": "UTF-8",
      "fieldDelimiter": ","
    }
  }
}
```

---

## Writer 配置

### MySQL Writer

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
      "preSql": [
        "TRUNCATE TABLE target_table"
      ],
      "postSql": [
        "UPDATE stats SET last_sync = NOW()"
      ],
      "batchSize": 1000,
      "connection": [
        {
          "jdbcUrl": "jdbc:mysql://localhost:3306/mydb",
          "table": ["target_table"]
        }
      ]
    }
  }
}
```

### HDFS Writer

```json
{
  "writer": {
    "name": "hdfswriter",
    "parameter": {
      "defaultFS": "hdfs://namenode:9000",
      "path": "/data/output",
      "fileName": "part",
      "column": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"}
      ],
      "fileType": "parquet",
      "writeMode": "truncate",
      "fieldDelimiter": "\t",
      "compress": "SNAPPY"
    }
  }
}
```

### Elasticsearch Writer

```json
{
  "writer": {
    "name": "elasticsearchwriter",
    "parameter": {
      "endpoint": "http://localhost:9200",
      "index": "my_index",
      "indexType": "_doc",
      "column": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"}
      ],
      "cleanup": true,
      "batchSize": 1000
    }
  }
}
```

---

## Job 配置

### 完整 Job 配置

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
            "column": ["*"],
            "connection": [
              {
                "jdbcUrl": ["jdbc:mysql://localhost:3306/source_db"],
                "table": ["source_table"]
              }
            ]
          }
        },
        "writer": {
          "name": "mysqlwriter",
          "parameter": {
            "username": "root",
            "password": "password",
            "column": ["*"],
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://localhost:3306/target_db",
                "table": ["target_table"]
              }
            ],
            "preSql": ["DELETE FROM target_table WHERE create_time < NOW()"]
          }
        }
      }
    ],
    "setting": {
      "speed": {
        "channel": 4,
        "byte": 10485760
      },
      "errorLimit": {
        "percentage": 0.02,
        "record": 100
      },
      "log": {
        "print": true
      }
    }
  }
}
```

### 性能调优

```json
{
  "setting": {
    "speed": {
      "channel": 8,          // 并发通道数
      "byte": 52428800,      // 每秒5MB
      "record": 10000        // 每秒1万条
    },
    "errorLimit": {
      "percentage": 0.05,    // 5% 错误率容忍
      "record": 1000         // 最多1000条错误
    }
  }
}
```

---

## 高级特性

### 增量同步

```json
{
  "job": {
    "content": [
      {
        "reader": {
          "name": "mysqlreader",
          "parameter": {
            "where": "update_time > '${last_sync_time}'"
          }
        }
      }
    ]
  }
}
```

### 条件过滤

```json
{
  "reader": {
    "name": "mysqlreader",
    "parameter": {
      "where": "status = 'active' AND create_time >= '2024-01-01'"
    }
  }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-best-practices.md](03-best-practices.md) | 最佳实践 |
| [README.md](README.md) | 索引文档 |
