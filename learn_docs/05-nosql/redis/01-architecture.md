# Redis 架构详解

## 目录

- [概述](#概述)
- [核心概念](#核心概念)
- [架构模式](#架构模式)
- [数据结构](#数据结构)
- [线程模型](#线程模型)

---

## 概述

Redis (Remote Dictionary Server) 是一个高性能的内存键值数据库。

### 核心特性

| 特性 | 说明 |
|------|------|
| **高性能** | 单线程 + 内存操作 |
| **多数据结构** | String, List, Hash, Set, ZSet 等 |
| **持久化** | RDB + AOF |
| **高可用** | 主从 + 哨兵 + 集群 |
| **发布订阅** | 支持消息队列 |

---

## 核心概念

### 数据类型

```
┌─────────────────────────────────────────────────────────────────┐
│                      Redis 数据类型                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  String     │  字符串，最基础类型                               │
│  Hash       │  散列，适合存储对象                               │
│  List       │  列表，顺序集合                                   │
│  Set        │  集合，无序唯一                                   │
│  ZSet       │  有序集合，带分数                                 │
│  Bitmaps    │  位图，适合统计                                   │
│  HyperLogLog│  基数估算                                         │
│  Geospatial │  地理位置                                         │
│  Stream     │  流数据，消息队列                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 核心术语

| 术语 | 说明 |
|------|------|
| **Key** | 键名 |
| **Value** | 值 |
| **Expiry** | 过期时间 |
| **TTL** | 生存时间 |
| **Pipeline** | 管道批量操作 |

---

## 架构模式

### 单机模式

```
┌─────────────────────────┐
│       Redis Server      │
│  ┌───────────────────┐  │
│  │    单线程模型     │  │
│  │  - 命令执行       │  │
│  │  - 网络 I/O       │  │
│  └───────────────────┘  │
│  ┌───────────────────┐  │
│  │    内存存储       │  │
│  └───────────────────┘  │
└─────────────────────────┘
```

### 主从模式

```
┌─────────────────┐         ┌─────────────────┐
│   Master        │ ──────► │    Slave 1      │
│   (读写)         │  同步   │   (读)          │
└─────────────────┘         └─────────────────┘
        │
        │                   ┌─────────────────┐
        └──────────────────►│    Slave 2      │
              同步          │   (读)          │
                           └─────────────────┘
```

### 哨兵模式

```
┌─────────────────┐
│   Sentinel      │ 监控
│   (选举/故障转移) │
└────────┬────────┘
         │
    ┌────┴────┬────────────┐
    ▼         ▼            ▼
┌───────┐ ┌───────┐  ┌──────────┐
│ Master│ │Slave 1│  │  Slave 2 │
└───────┘ └───────┘  └──────────┘
```

### 集群模式

```
┌─────────────────────────────────────────────────┐
│                    Cluster                       │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐         │
│  │ Slot 0  │  │Slot 1   │  │Slot 2   │         │
│  │ Master  │  │ Master  │  │ Master  │         │
│  │ Slave   │  │ Slave   │  │ Slave   │         │
│  └─────────┘  └─────────┘  └─────────┘         │
│                                                 │
│  数据分片: 16384 个槽 (0-16383)                 │
│  客户端分片: hash(key) % 16384                  │
└─────────────────────────────────────────────────┘
```

---

## 数据结构

### String

```java
// String 操作
jedis.set("key", "value");        // 设置
String val = jedis.get("key");    // 获取

// 批量操作
jedis.mset("k1", "v1", "k2", "v2");
List<String> vals = jedis.mget("k1", "k2");

// 过期设置
jedis.setex("key", 60, "value");  // 60秒过期
jedis.psetex("key", 60000, "value"); // 毫秒过期

// 数字操作
jedis.incr("counter");            // +1
jedis.incrBy("counter", 10);      // +10
jedis.decr("counter");            // -1
```

### Hash

```java
// Hash 操作
jedis.hset("user:1001", "name", "Alice");
jedis.hset("user:1001", "age", "25");

// 获取
String name = jedis.hget("user:1001", "name");
Map<String, String> user = jedis.hgetAll("user:1001");

// 批量操作
jedis.hmset("user:1002", map);
jedis.hmget("user:1002", "name", "age");
```

### List

```java
// 列表操作
jedis.lpush("queue", "task1", "task2");  // 左侧插入
jedis.rpush("queue", "task3");           // 右侧插入

// 获取
String task = jedis.lpop("queue");       // 左侧弹出
String task2 = jedis.rpop("queue");      // 右侧弹出

List<String> tasks = jedis.lrange("queue", 0, -1);
```

### Set

```java
// 集合操作
jedis.sadd("tags", "java", "redis", "mysql");

// 获取
Set<String> tags = jedis.smembers("tags");
boolean exists = jedis.sismember("tags", "redis");

// 集合运算
jedis.sinter("tags1", "tags2");  // 交集
jedis.sunion("tags1", "tags2");  // 并集
jedis.sdiff("tags1", "tags2");   // 差集
```

### ZSet

```java
// 有序集合
jedis.zadd("ranking", 100, "Alice");
jedis.zadd("ranking", 90, "Bob");
jedis.zadd("ranking", 95, "Charlie");

// 获取
List<String> top3 = jedis.zrevrange("ranking", 0, 2);  // 前3名
Double score = jedis.zscore("ranking", "Alice");       // 分数
Long rank = jedis.zrevrank("ranking", "Alice");        // 排名
```

---

## 线程模型

### 单线程模型

```java
// Redis 单线程模型特点:
// 1. 所有命令串行执行
// 2. 无需考虑并发安全
// 3. 避免锁竞争开销
// 4. 内存操作，性能极高

// 核心流程:
// Client → EventLoop → Command Parser → Command Executor → Response → Client
```

### I/O 多路复用

```java
// 基于 epoll/kqueue/select
// 单线程监听多个 socket
// 有数据时执行对应命令
// 高效处理大量并发连接
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [02-data-operations.md](02-data-operations.md) | 数据操作指南 |
| [03-cluster.md](03-cluster.md) | 集群部署 |
| [04-optimization.md](04-optimization.md) | 性能优化 |
| [README.md](README.md) | 索引文档 |
