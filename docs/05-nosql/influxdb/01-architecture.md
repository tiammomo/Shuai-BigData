# InfluxDB 架构详解

## 目录

- [概述](#概述)
- [核心概念](#核心概念)
- [数据模型](#数据模型)
- [架构原理](#架构原理)
- [存储引擎](#存储引擎)

---

## 概述

InfluxDB 是一个开源的时间序列数据库，专为时间序列数据优化。

### 核心特性

| 特性 | 说明 |
|------|------|
| **时序优化** | 专为时间序列设计 |
| **高速写入** | 高吞吐量 |
| **高效压缩** | 专用压缩算法 |
| **连续查询** | 自动聚合 |
| **保留策略** | 自动数据清理 |

---

## 核心概念

### 核心术语

```
┌─────────────────────────────────────────────────────────────────┐
│                    InfluxDB 核心概念                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Measurement   │  测量名称，相当于表                            │
│  Tag          │  索引字段，用于查询过滤                         │
│  Field        │  数据字段，存储实际值                           │
│  Timestamp    │  时间戳，时间序列核心                           │
│  Point        │  数据点 (Tags + Fields + Timestamp)             │
│  Series       │  序列 (Measurement + Tag组合)                   │
│  Retention    │  保留策略，数据自动删除策略                     │
│  Continuous   │  连续查询，自动执行聚合查询                     │
│  Query                                             │             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 数据模型

### 数据结构

```
┌─────────────────────────────────────────────────────────────────┐
│                    InfluxDB 数据结构                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Measurement: cpu                                               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ time                │ host   │ region │ usage_user       │   │
│  │ ─────────────────── │ ────── │ ────── │ ─────────────    │   │
│  │ 2024-01-10T10:00:00Z│ server1│ cn-bj  │ 25.5             │   │
│  │ 2024-01-10T10:00:05Z│ server1│ cn-bj  │ 23.2             │   │
│  │ 2024-01-10T10:00:00Z│ server2│ cn-sh  │ 42.1             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Tag:    host, region  (索引，可过滤)                           │
│  Field:  usage_user    (数据，存储值)                           │
│  Time:   timestamp     (主键，自动生成)                         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Line Protocol

```
# 格式: measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 timestamp

# 示例
cpu,host=server1,region=cn-bj usage_user=25.5,usage_system=10.2 1704880800000000000

# 字段说明:
# cpu          - measurement
# host=server1  - tag
# region=cn-bj  - tag
# usage_user=25.5 - field
# usage_system=10.2 - field
# 1704880800000000000 - timestamp (纳秒)
```

---

## 架构原理

### TSM 存储

```
┌─────────────────────────────────────────────────────────────────┐
│                    TSM (Time Structured Merge Tree)             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  写入流程:                                                       │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐         │
│  │  In-Memory  │───►│    Cache    │───►│   TSM Files │         │
│  │   Shard     │    │  (MemStore) │    │  (Compacted)│         │
│  └─────────────┘    └─────────────┘    └─────────────┘         │
│                                                                 │
│  TSM File 结构:                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Header  │  Data Blocks  │  Index Block  │  Footer     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  特点:                                                          │
│  - 列式存储                                                     │
│  - 压缩友好                                                     │
│  - 顺序写入                                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 存储引擎

### Shard 管理

```sql
-- 创建保留策略
CREATE RETENTION POLICY "30d_policy" ON "mydb"
DURATION 30d
REPLICATION 1
SHARD DURATION 1d;

-- 查看保留策略
SHOW RETENTION POLICIES;

-- 修改保留策略
ALTER RETENTION POLICY "30d_policy" ON "mydb"
DURATION 60d
SHARD DURATION 7d;
```

### Continuous Query

```sql
-- 创建连续查询
CREATE CONTINUOUS QUERY "cq_1m_avg" ON "mydb"
BEGIN
  SELECT mean(usage_user) AS avg_usage
  INTO "cpu_1m_avg"
  FROM "cpu"
  GROUP BY time(1m), host
END;

-- 查看连续查询
SHOW CONTINUOUS QUERIES;

-- 删除连续查询
DROP CONTINUOUS QUERY "cq_1m_avg" ON "mydb";
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [02-data-operations.md](02-data-operations.md) | 数据操作指南 |
| [03-query-guide.md](03-query-guide.md) | 查询指南 |
| [README.md](README.md) | 索引文档 |
