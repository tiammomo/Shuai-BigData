# SeaTunnel 作业配置

## 目录

- [基本配置](#基本配置)
- [Source 配置](#source-配置)
- [Transform 配置](#transform-配置)
- [Sink 配置](#sink-配置)
- [复杂场景](#复杂场景)

---

## 基本配置

### 环境配置

```hocon
env {
  job.name = "my-job"
  job.mode = "batch"  // batch 或 streaming
  parallelism = 4
  checkpoint.interval = 60000
  job.content = [
    source, transform, sink
  ]
}
```

### 执行模式

```hocon
# 批处理模式
env {
  job.mode = "batch"
  batch.size = 10000
}

# 流处理模式
env {
  job.mode = "streaming"
  checkpoint.interval = 60000
  watermark.emit.strategy = "current_watermark"
}
```

---

## Source 配置

### MySQL CDC

```hocon
source {
  MySQL-CDC {
    url = "jdbc:mysql://localhost:3306"
    username = "root"
    password = "password"
    table-names = ["mydb.users", "mydb.orders"]
    base-url = "jdbc:mysql://localhost:3306"

    # 可选配置
    startup.mode = "initial"  // initial, latest, specific
    start-up.specific-offset.file = "binlog.000001"
    start-up.specific-offset.pos = 1234

    # 并行度
    source parallelism = 4

    # 内存管理
    fetch.size = 10000
    connect.timeout = 30000
  }
}
```

### Kafka

```hocon
source {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topics = "topic1,topic2"
    consumer.group = "seatunnel-group"
    mode = "text"

    # 消费配置
    auto.offset.reset = "earliest"
    enable.auto.commit = true
    auto.commit.interval.ms = 5000

    # 订阅模式
    subscribe.pattern = "topic.*"

    # 数据格式
    format = "json"
    json.map.empty.entry.as.null = true
  }
}
```

### HDFS

```hocon
source {
  HdfsFile {
    path = "/data/input"
    file_format_type = "parquet"

    # 文件格式
    file_format_type = "text"  // text, csv, json, parquet, orc

    # 分区
    partition_by = ["date", "hour"]
    partition_date_format = "yyyy-MM-dd"
    partition_time_format = "HH"

    # 并行度
    source parallelism = 4
  }
}
```

---

## Transform 配置

### SQL Transform

```hocon
transform {
  Sql {
    sql = """
      SELECT
        id,
        name,
        age,
        CASE
          WHEN age >= 60 THEN 'elder'
          WHEN age >= 18 THEN 'adult'
          ELSE 'minor'
        END AS age_group,
        timestamp
      FROM source_table
      WHERE status = 'active'
    """
  }
}
```

### Field Mapper

```hocon
transform {
  FieldMapper {
    source_field = "user_name"
    result_field = "username"
  }
}

# 或批量映射
transform {
  FieldMapper {
    field_map = {
      "old_name_1" = "new_name_1"
      "old_name_2" = "new_name_2"
    }
  }
}
```

### Filter

```hocon
transform {
  Filter {
    source_field = "*"
    result_field = "*"

    # 条件过滤
    condition = "age >= 18 AND status = 'active'"
  }
}
```

### 其他 Transform

```hocon
# 移除字段
transform {
  RemoveField {
    fields = ["password", "secret_token"]
  }
}

# 添加字段
transform {
  AddField {
    fields = {
      "created_at" = "NOW()"
      "processed" = "true"
    }
  }
}

# 转换字段类型
transform {
  ConvertType {
    field_type = {
      "age" = "int"
      "price" = "double"
      "active" = "boolean"
    }
  }
}
```

---

## Sink 配置

### MySQL

```hocon
sink {
  JDBC {
    url = "jdbc:mysql://localhost:3306/mydb"
    username = "root"
    password = "password"
    table = "target_table"

    # 写入模式
    save_mode = "update"  // append, drop, truncate, update, create

    # 批量写入
    batch.size = 1000
    batch.interval = 5000

    # 主键
    primary_keys = ["id"]

    # 兼容性
    compatible_mode = "mysql"
  }
}
```

### Kafka

```hocon
sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "output-topic"
    format = "json"

    # 分区策略
    partition = 0  // 固定分区
    partition_key_field = "user_id"  // 按字段分区

    # 语义
    semantics = "EXACTLY_ONCE"  // NONE, AT_LEAST_ONCE, EXACTLY_ONCE
  }
}
```

### HDFS

```hocon
sink {
  HdfsFile {
    path = "/data/output"
    file_format_type = "parquet"
    file_name_expression = "transaction_${time}"

    # 分区
    partition_by = ["date", "type"]

    # 文件管理
    overwrite = true
    max_rows_in_memory = 1000
    rolling_policy = {
      roll_interval = 3600
      roll_size = 128MB
    }
  }
}
```

---

## 复杂场景

### 多 Source 聚合

```hocon
env {
  parallelism = 4
}

source {
  orders {
    # ...
  }

  users {
    # ...
  }
}

transform {
  Sql {
    sql = """
      SELECT
        o.order_id,
        o.user_id,
        u.name,
        u.email,
        o.amount
      FROM orders o
      JOIN users u ON o.user_id = u.user_id
    """
  }
}

sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "orders_with_users"
  }
}
```

### 条件路由

```hocon
transform {
  Router {
    route {
      condition = "status = 'completed'"
      sink {
        Kafka {
          topic = "completed_orders"
        }
      }
    }

    route {
      condition = "status = 'cancelled'"
      sink {
        Kafka {
          topic = "cancelled_orders"
        }
      }
    }
  }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-connector-guide.md](03-connector-guide.md) | 连接器指南 |
| [04-best-practices.md](04-best-practices.md) | 最佳实践 |
| [README.md](README.md) | 索引文档 |
