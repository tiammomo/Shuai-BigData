# Elasticsearch 查询指南

## 目录

- [查询类型](#查询类型)
- [全文查询](#全文查询)
- [精确查询](#精确查询)
- [聚合查询](#聚合查询)
- [复杂查询](#复杂查询)

---

## 查询类型

```
┌─────────────────────────────────────────────────────────────────┐
│                    Elasticsearch 查询类型                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┬────────────────────────────────────────┐  │
│  │  叶子查询       │  复合查询                               │  │
│  ├─────────────────┼────────────────────────────────────────┤  │
│  │  - match        │  - bool                                │  │
│  │  - term         │  - dis_max                             │  │
│  │  - range        │  - function_score                      │  │
│  │  - exists       │  - bool + filter                       │  │
│  │  - ids          │  - nested                              │  │
│  └─────────────────┴────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 全文查询

### match 查询

```json
// 基本 match 查询
GET /products/_search
{
  "query": {
    "match": {
      "name": "iphone"
    }
  }
}

// 短语匹配 (保持词序)
GET /products/_search
{
  "query": {
    "match_phrase": {
      "name": "iphone 15"
    }
  }
}

// 多字段匹配
GET /products/_search
{
  "query": {
    "multi_match": {
      "query": "iphone",
      "fields": ["name", "description", "brand"]
    }
  }
}

// 带运算符
GET /products/_search
{
  "query": {
    "match": {
      "name": "iphone OR samsung"
    }
  }
}
```

### query_string

```json
// 复杂查询语法
GET /products/_search
{
  "query": {
    "query_string": {
      "query": "(name:iphone OR name:samsung) AND price:[100 TO 500]",
      "fields": ["name", "brand"],
      "default_operator": "AND"
    }
  }
}
```

---

## 精确查询

### term 查询

```json
// 精确值匹配
GET /products/_search
{
  "query": {
    "term": {
      "brand.keyword": "Apple"
    }
  }
}

// 多值精确匹配
GET /products/_search
{
  "query": {
    "terms": {
      "brand": ["Apple", "Samsung"]
    }
  }
}
```

### range 查询

```json
// 范围查询
GET /products/_search
{
  "query": {
    "range": {
      "price": {
        "gte": 100,
        "lte": 500
      }
    }
  }
}

// 日期范围
GET /orders/_search
{
  "query": {
    "range": {
      "created_at": {
        "gte": "2024-01-01",
        "lte": "2024-01-31"
      }
    }
  }
}

// 日期数学
GET /orders/_search
{
  "query": {
    "range": {
      "created_at": {
        "gte": "now-7d/d",
        "lte": "now/d"
      }
    }
  }
}
```

### exists 查询

```json
// 字段存在查询
GET /products/_search
{
  "query": {
    "exists": {
      "field": "description"
    }
  }
}
```

---

## 聚合查询

### 指标聚合

```json
// 统计聚合
GET /products/_search
{
  "size": 0,
  "aggs": {
    "price_stats": {
      "stats": {
        "field": "price"
      }
    },
    "avg_price": {
      "avg": {
        "field": "price"
      }
    },
    "max_price": {
      "max": {
        "field": "price"
      }
    },
    "min_price": {
      "min": {
        "field": "price"
      }
    },
    "price_percentiles": {
      "percentiles": {
        "field": "price",
        "percents": [25, 50, 75, 95, 99]
      }
    }
  }
}
```

### 桶聚合

```json
// 词条聚合
GET /products/_search
{
  "size": 0,
  "aggs": {
    "brands": {
      "terms": {
        "field": "brand.keyword",
        "size": 10
      }
    }
  }
}

// 范围聚合
GET /products/_search
{
  "size": 0,
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price",
        "ranges": [
          {"key": "cheap", "to": 100},
          {"key": "medium", "from": 100, "to": 500},
          {"key": "expensive", "from": 500}
        ]
      }
    }
  }
}

// 日期直方图
GET /orders/_search
{
  "size": 0,
  "aggs": {
    "orders_over_time": {
      "date_histogram": {
        "field": "created_at",
        "calendar_interval": "day"
      }
    }
  }
}
```

### 嵌套聚合

```json
// 嵌套聚合 (按品牌分组，再统计价格)
GET /products/_search
{
  "size": 0,
  "aggs": {
    "brands": {
      "terms": {
        "field": "brand.keyword",
        "size": 10
      },
      "aggs": {
        "price_stats": {
          "stats": {
            "field": "price"
          }
        },
        "avg_price": {
          "avg": {
            "field": "price"
          }
        }
      }
    }
  }
}
```

---

## 复杂查询

### bool 查询

```json
// must (AND)
GET /products/_search
{
  "query": {
    "bool": {
      "must": [
        {"match": {"name": "iphone"}},
        {"range": {"price": {"gte": 500}}}
      ]
    }
  }
}

// should (OR)
GET /products/_search
{
  "query": {
    "bool": {
      "should": [
        {"match": {"name": "iphone"}},
        {"match": {"name": "samsung"}}
      ],
      "minimum_should_match": 1
    }
  }
}

// must_not (NOT)
GET /products/_search
{
  "query": {
    "bool": {
      "must_not": [
        {"term": {"brand.keyword": "Xiaomi"}}
      ]
    }
  }
}

// filter (不带评分)
GET /products/_search
{
  "query": {
    "bool": {
      "filter": [
        {"term": {"status": "active"}},
        {"range": {"price": {"lte": 1000}}}
      ]
    }
  }
}

// 综合示例
GET /products/_search
{
  "query": {
    "bool": {
      "must": [
        {"multi_match": {
          "query": "smartphone",
          "fields": ["name", "description"]
        }}
      ],
      "filter": [
        {"range": {"price": {"gte": 300, "lte": 1500}}},
        {"term": {"status": "active"}}
      ],
      "should": [
        {"term": {"brand.keyword": "Apple"}},
        {"term": {"brand.keyword": "Samsung"}}
      ],
      "minimum_should_match": 1
    }
  }
}
```

### nested 查询

```json
// 嵌套查询
GET /products/_search
{
  "query": {
    "nested": {
      "path": "reviews",
      "query": {
        "bool": {
          "must": [
            {"range": {"reviews.rating": {"gte": 4}}}
          ]
        }
      }
    }
  }
}

// 嵌套聚合
GET /products/_search
{
  "size": 0,
  "aggs": {
    "nested_reviews": {
      "nested": {
        "path": "reviews"
      },
      "aggs": {
        "avg_rating": {
          "avg": {
            "field": "reviews.rating"
          }
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
| [03-operations.md](03-operations.md) | 运维指南 |
| [README.md](README.md) | 索引文档 |
