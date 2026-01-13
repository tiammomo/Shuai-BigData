# Flink CDC 连接器配置

## 目录

- [MySQL CDC](#mysql-cdc)
- [PostgreSQL CDC](#postgresql-cdc)
- [MongoDB CDC](#mongodb-cdc)
- [整库同步](#整库同步)

---

## MySQL CDC

### Flink SQL

```sql
-- 创建 MySQL CDC 表
CREATE TABLE mysql_source (
    id BIGINT,
    name STRING,
    age INT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = 'password',
    'database-name' = 'mydb',
    'table-name' = 'users',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '8096'
);

-- 写入 MySQL
CREATE TABLE mysql_sink (
    id BIGINT,
    name STRING,
    age INT,
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/target_db',
    'username' = 'root',
    'password' = 'password',
    'table-name' = 'users'
);

-- 数据同步
INSERT INTO mysql_sink
SELECT * FROM mysql_source;
```

### DataStream API

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);

MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
    .hostname("localhost")
    .port(3306)
    .databaseList("mydb")
    .tableList("mydb.users", "mydb.orders")
    .username("root")
    .password("password")
    .deserializer(new JsonDebeziumDeserializationSchema())
    .startupOptions(StartupOptions.initial())  // 全量 + 增量
    // .startupOptions(StartupOptions.latest())  // 仅增量
    .build();

env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
   .print();

env.execute("MySQL CDC");
```

### 整库同步

```sql
-- 整库同步
CREATE TABLE source_db (
    -- 使用正则匹配表
    `.*` STRING
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3306',
    'username' = 'root',
    'password' = 'password',
    'database-name' = 'mydb',
    'table-name' = '.*',
    'scan.incremental.snapshot.enabled' = 'true'
);
```

---

## PostgreSQL CDC

### Flink SQL

```sql
-- PostgreSQL CDC 表
CREATE TABLE pg_source (
    id BIGINT,
    data JSONB,
    create_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'localhost',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'password',
    'database-name' = 'mydb',
    'schema-name' = 'public',
    'table-name' = 'events',
    'publication.autocreate.enabled' = 'true',
    'slot.name' = 'flink_cdc'
);
```

### 逻辑复制配置

```sql
-- 创建发布
CREATE PUBLICATION flink_publication FOR ALL TABLES;

-- 或指定表
CREATE PUBLICATION flink_publication FOR TABLE users, orders;
```

---

## MongoDB CDC

### Flink SQL

```sql
-- MongoDB CDC 表
CREATE TABLE mongo_source (
    _id STRING,
    name STRING,
    age INT,
    PRIMARY KEY (_id) NOT ENFORCED
) WITH (
    'connector' = 'mongodb-cdc',
    'hosts' = 'localhost:27017',
    'username' = 'admin',
    'password' = 'password',
    'database' = 'mydb',
    'collection' = 'users',
    'scan.startup.mode' = 'latest'
);
```

### DataStream API

```java
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.connectors.mongodb.deserializer.MongoDBDeserializationSchema;

MongoDBSource<String> mongoSource = MongoDBSource.<String>builder()
    .hosts("localhost:27017")
    .database("mydb")
    .collection("users")
    .username("admin")
    .password("password")
    .deserializer(new MongoDBDeserializationSchema())
    .startupOptions(StartupOptions.initial())
    .build();
```

---

## 整库同步

### 多表同步

```java
// 整库同步
MySqlSource<String> source = MySqlSource.<String>builder()
    .hostname("localhost")
    .port(3306)
    .databaseList("mydb")
    .tableList("mydb.*")  // 所有表
    .username("root")
    .password("password")
    .deserializer(new JsonDebeziumDeserializationSchema())
    .build();
```

### Schema 变更处理

```java
// Schema 变更监听
env.getConfig().enableSchemaEvolution();
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-best-practices.md](03-best-practices.md) | 最佳实践 |
| [README.md](README.md) | 索引文档 |
