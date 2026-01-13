# Redis 中文文档

> Redis 是一个高性能的内存键值数据库。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作指南 |
| [03-cluster.md](03-cluster.md) | 集群部署 |

## 快速入门

```java
// Maven 依赖
<dependency>
    <groupId>redis.clients</groupId>
    <artifactId>jedis</artifactId>
    <version>5.1.0</version>
</dependency>
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     Redis 数据类型                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      String (字符串)                     │   │
│  │  - 简单键值                                             │   │
│  │  - 缓存、计数器                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Hash (哈希)                         │   │
│  │  - 字段-值映射                                          │   │
│  │  - 存储对象                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      List (列表)                         │   │
│  │  - 有序集合                                             │   │
│  │  - 队列、栈                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Set (集合)                          │   │
│  │  - 无序去重                                             │   │
│  │  - 标签、好友关系                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Sorted Set (有序集合)               │   │
│  │  - 带分数排序                                           │   │
│  │  - 排行榜、优先级队列                                   │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Java API 示例

```java
// Jedis 连接
Jedis jedis = new Jedis("localhost", 6379);

// String 操作
jedis.set("name", "John");
String value = jedis.get("name");

// Hash 操作
jedis.hset("user:1", "name", "John");
jedis.hset("user:1", "age", "25");
Map<String, String> user = jedis.hgetAll("user:1");

// List 操作
jedis.lpush("queue", "task1", "task2");
String task = jedis.rpop("queue");

// Set 操作
jedis.sadd("tags", "java", "spark", "flink");
Set<String> tags = jedis.smembers("tags");

// Sorted Set 操作
jedis.zadd("ranking", 100, "player1");
jedis.zadd("ranking", 200, "player2");
List<String> topPlayers = jedis.zrevrange("ranking", 0, 9);
```

## 相关资源

- [官方文档](https://redis.io/documentation)
- [01-architecture.md](01-architecture.md)
