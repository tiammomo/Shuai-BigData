# Elasticsearch 运维指南

## 目录

- [集群管理](#集群管理)
- [索引管理](#索引管理)
- [性能优化](#性能优化)
- [监控告警](#监控告警)
- [故障处理](#故障处理)

---

## 集群管理

### 节点角色

```yaml
# elasticsearch.yml
# 主节点 (Master Node)
node.master: true
node.data: false
node.ingest: false

# 数据节点 (Data Node)
node.master: false
node.data: true
node.ingest: false

# 协调节点 (Coordinating Node)
node.master: false
node.data: false
node.ingest: false

# 混合节点
node.master: true
node.data: true
node.ingest: true
```

### 集群配置

```bash
# 查看集群健康
curl -X GET "localhost:9200/_cluster/health?pretty"

# 查看节点列表
curl -X GET "localhost:9200/_cat/nodes?v"

# 查看集群状态
curl -X GET "localhost:9200/_cluster/state?pretty"

# 查看索引分片
curl -X GET "localhost:9200/_cat/shards?v"
```

### 集群设置

```json
// 动态设置
PUT /_cluster/settings
{
  "persistent": {
    "cluster.routing.allocation.disk.threshold_enabled": true,
    "cluster.routing.allocation.disk.watermark.low": "85%",
    "cluster.routing.allocation.disk.watermark.high": "90%"
  }
}

// 查看设置
GET /_cluster/settings
```

---

## 索引管理

### 索引模板

```json
PUT /_index_template/products
{
  "index_patterns": ["products*"],
  "template": {
    "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 1,
      "refresh_interval": "1s"
    },
    "mappings": {
      "properties": {
        "name": {
          "type": "text",
          "analyzer": "standard"
        },
        "price": {
          "type": "float"
        },
        "brand": {
          "type": "keyword"
        },
        "created_at": {
          "type": "date"
        }
      }
    }
  }
}
```

### 索引别名

```json
// 创建别名
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "products_v1",
        "alias": "products"
      }
    }
  ]
}

// 切换别名
POST /_aliases
{
  "actions": [
    {"remove": {"index": "products_v1", "alias": "products"}},
    {"add": {"index": "products_v2", "alias": "products"}}
  ]
}
```

### ILM 策略

```json
PUT /_ilm/policy/hot-warm-delete
{
  "policy": {
    "phases": {
      "hot": {
        "min_age": "0ms",
        "actions": {
          "rollover": {
            "max_age": "1d",
            "max_size": "50gb"
          },
          "set_priority": {
            "priority": 100
          }
        }
      },
      "warm": {
        "min_age": "7d",
        "actions": {
          "shrink": {
            "number_of_shards": 1
          },
          "forcemerge": {
            "max_num_segments": 1
          },
          "set_priority": {
            "priority": 50
          }
        }
      },
      "delete": {
        "min_age": "30d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}
```

---

## 性能优化

### 分片优化

```json
// 创建索引时指定分片
PUT /my-index
{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1
  }
}

// 分片分配
PUT /_cluster/settings
{
  "persistent": {
    "cluster.routing.allocation.awareness.attributes": "rack_id",
    "cluster.routing.allocation.node_concurrent_incoming_recoveries": 2,
    "cluster.routing.allocation.node_concurrent_outgoing_recoveries": 2
  }
}
```

### 缓存优化

```json
// 索引级缓存设置
PUT /my-index/_settings
{
  "index": {
    "queries.cache.enabled": true,
    "fielddata.cache.size": "30%",
    "request_cache.enabled": true
  }
}
```

### 查询优化

```json
// 使用 filter cache
GET /products/_search
{
  "query": {
    "bool": {
      "filter": [
        {"term": {"status": "active"}},
        {"range": {"price": {"gte": 100}}}
      ]
    }
  }
}

// 关闭_source 的不需要字段
GET /products/_search
{
  "_source": ["name", "price"],
  "query": {
    "match": {"name": "iphone"}
  }
}
```

---

## 监控告警

### 监控指标

```bash
# 节点统计
curl -X GET "localhost:9200/_nodes/stats?pretty"

# 索引统计
curl -X GET "localhost:9200/_stats?pretty"

# 索引状态
curl -X GET "localhost:9200/_cat/indices?v&health=green"
```

### 关键指标

| 指标 | 说明 | 告警阈值 |
|------|------|---------|
| cluster.health.status | 集群状态 | red |
| disk.used_percent | 磁盘使用率 | > 85% |
| fielddata.cache.size | 字段缓存 | > 60% |
| thread_pool.queue | 线程队列 | > 100 |
| search.latency | 查询延迟 | > 1s |

---

## 故障处理

### 节点故障

```bash
# 查看节点状态
curl -X GET "localhost:9200/_cat/nodes?v"

# 查看分片状态
curl -X GET "localhost:9200/_cat/shards?v"

# 查看未分配分片
curl -X GET "localhost:9200/_cat/shards?h=index,shard,prirep,state,unassigned.reason"
```

### 分片恢复

```json
// 手动分配分片
POST /_cluster/reroute
{
  "commands": [
    {
      "allocate_stripe": {
        "index": "my-index",
        "shard": 0,
        "node": "node-name"
      }
    }
  ]
}

// 强制重新分配
POST /_cluster/reroute?retry_failed=true
```

### 磁盘不足

```json
// 清理索引
DELETE /old-index

// 强制合并
POST /my-index/_forcemerge

// 调整 watermark
PUT /_cluster/settings
{
  "persistent": {
    "cluster.routing.allocation.disk.watermark.low": "80%",
    "cluster.routing.allocation.disk.watermark.high": "85%"
  }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-query.md](02-query.md) | 查询指南 |
| [README.md](README.md) | 索引文档 |
