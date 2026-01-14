# Elasticsearch 中文文档

> Elasticsearch 是一个分布式搜索和分析引擎。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |

## 快速入门

```java
// Maven 依赖
<dependency>
    <groupId>co.elastic.clients</groupId>
    <artifactId>elasticsearch-java</artifactId>
    <version>8.11.0</version>
</dependency>
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     Elasticsearch 核心概念                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Index (索引)                        │   │
│  │  - 文档存储容器                                          │   │
│  │  - 类似于数据库                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Document (文档)                     │   │
│  │  - JSON 格式数据                                        │   │
│  │  - 类似于数据库行                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Mapping (映射)                      │   │
│  │  - Schema 定义                                          │   │
│  │  - 字段类型                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Shard (分片)                        │   │
│  │  - 数据分片                                             │   │
│  │  - 分布式存储                                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Java API 示例

```java
// 创建索引
CreateIndexRequest request = new CreateIndexRequest.Builder()
    .index("users")
    .build();
client.indices().create(request);

// 索引文档
IndexRequest indexRequest = new IndexRequest.Builder()
    .index("users")
    .id("1")
    .document(User.class)
    .build();
client.index(indexRequest);

// 搜索
SearchRequest searchRequest = new SearchRequest.Builder()
    .index("users")
    .query(q -> q.match(m -> m.field("name").query("John")))
    .build();
SearchResponse<User> response = client.search(searchRequest, User.class);
```

## 相关资源

- [官方文档](https://www.elastic.co/guide/)
- [01-architecture.md](01-architecture.md)
