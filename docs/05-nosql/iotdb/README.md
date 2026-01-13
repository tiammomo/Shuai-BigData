# IoTDB 中文文档

> IoTDB 是一个面向工业物联网的时序数据库。

## 文档列表

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |

## 快速入门

```java
// Maven 依赖
<dependency>
    <groupId>org.apache.iotdb</groupId>
    <artifactId>iotdb-jdbc</artifactId>
    <version>1.2.0</version>
</dependency>
```

## 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                     IoTDB 核心概念                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Database (数据库)                   │   │
│  │  - 逻辑命名空间                                          │   │
│  │  - 存储组                                                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Path (路径)                         │   │
│  │  - 设备/传感器路径                                       │   │
│  │  - 层级结构                                              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Time Series (时序)                  │   │
│  │  - 时间序列数据                                          │   │
│  │  - 传感器测量值                                          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Java API 示例

```java
// JDBC 连接
Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
Connection connection = DriverManager.getConnection(
    "jdbc:iotdb://localhost:6667/", "root", "root");

// 插入数据
Statement statement = connection.createStatement();
statement.execute(
    "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) " +
    "VALUES(2024-01-01 12:00:00, 25.6)"
);

// 查询数据
ResultSet resultSet = statement.executeQuery(
    "SELECT temperature FROM root.ln.wf01.wt01 " +
    "WHERE time >= 2024-01-01 12:00:00 AND time <= 2024-01-01 12:10:00"
);
```

## 相关资源

- [官方文档](https://iotdb.apache.org/docs/)
- [01-architecture.md](01-architecture.md)
