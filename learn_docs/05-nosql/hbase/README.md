# HBase 中文文档

> HBase 是一个分布式列式存储数据库，基于 Hadoop 构建。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |

## 快速入门

```java
// Maven 依赖
<dependency>
    <groupId>org.apache.hbase</groupId>
    <artifactId>hbase-client</artifactId>
    <version>2.5.0</version>
</dependency>
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     HBase 核心架构                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Table (表)                          │   │
│  │  - 行键 (Row Key)                                        │   │
│  │  - 列族 (Column Family)                                  │   │
│  │  - 列限定符 (Column Qualifier)                           │   │
│  │  - 时间戳 (Timestamp)                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Region (区域)                       │   │
│  │  - 行的分片                                             │   │
│  │  - 自动分区                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      HMaster                             │   │
│  │  - 元数据管理                                           │   │
│  │  - 区域协调                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      RegionServer                        │   │
│  │  - 数据服务                                             │   │
│  │  - 读写处理                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Java API 示例

```java
// 连接 HBase
Connection connection = ConnectionFactory.createConnection(config);
Table table = connection.getTable(TableName.valueOf("myTable"));

// 插入数据
Put put = new Put(Bytes.toBytes("row1"));
put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("John"));
table.put(put);

// 读取数据
Get get = new Get(Bytes.toBytes("row1"));
Result result = table.get(get);
```

## 相关资源

- [官方文档](https://hbase.apache.org/book.html)
- [01-architecture.md](01-architecture.md)
