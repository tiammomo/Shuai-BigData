# Flink Table API / SQL 详解

## 目录

- [环境配置](#环境配置)
- [表环境](#表环境)
- [表定义](#表定义)
- [查询操作](#查询操作)
- [窗口聚合](#窗口聚合)
- [连接操作](#连接操作)
- [DML 操作](#dml-操作)
- [类型系统](#类型系统)

---

## 环境配置

### Maven 依赖

```xml
<!-- Flink Table API -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-api-java-bridge</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- Flink Table Planner (Blink) -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-planner-blink_2.12</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- Flink Table Runtime -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-runtime-blink_2.12</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- Flink SQL Client -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-sql-client</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- 数据连接器 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-jdbc</artifactId>
    <version>3.1.0-1.18</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>${flink.version}</version>
</dependency>
```

### 依赖引入

```java
// Flink 1.18+ 使用新的依赖结构
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
```

---

## 表环境

### 创建表环境

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.descriptor.*;

/**
 * Flink 表环境配置
 */
public class TableEnvironmentExample {

    /**
     * 1. 流表环境 (最常用)
     *
     * 将 DataStream 转换为 Table
     */
    public static StreamTableEnvironment createStreamTableEnv() {
        // 创建流执行环境
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建流表环境
        StreamTableEnvironment tableEnv =
            StreamTableEnvironment.create(env);

        // 或通过环境配置创建
        EnvironmentSettings settings =
            EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        return StreamTableEnvironment.create(env, settings);
    }

    /**
     * 2. 批表环境
     *
     * 用于批处理场景
     */
    public static TableEnvironment createBatchTableEnv() {
        EnvironmentSettings settings =
            EnvironmentSettings
                .newInstance()
                .inBatchMode()
                .useBlinkPlanner()
                .build();

        return TableEnvironment.create(settings);
    }

    /**
     * 3. 通用表环境
     *
     * 不绑定执行环境
     */
    public static TableEnvironment createGeneralTableEnv() {
        EnvironmentSettings settings =
            EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .build();

        return TableEnvironment.create(settings);
    }

    /**
     * 4. 配置表环境
     */
    public static void configureTableEnv(StreamTableEnvironment tableEnv) {
        // 设置最大并行度
        tableEnv.getConfig().setMaxParallelism(16);

        // 设置最小并行度
        tableEnv.getConfig().setMinParallelism(2);

        // 设置空闲状态保留时间
        tableEnv.getConfig().setIdleStateRetention(
            java.time.Duration.ofHours(24)
        );

        // 启用 Shuffle 模式
        tableEnv.getConfig()
            .set("table.exec.shuffle-mode", "ALL_EDGES");
    }
}
```

---

## 表定义

### DDL 定义表

```java
import org.apache.flink.table.api.*;
import org.apache.flink.table.descriptors.*;

/**
 * 表定义方式
 */
public class TableDefinition {

    /**
     * 1. DDL 创建表
     */
    public static void createTableWithDDL(StreamTableEnvironment tableEnv) {
        // 创建 Kafka 表
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "  order_id BIGINT," +
            "  customer_id STRING," +
            "  amount DECIMAL(10, 2)," +
            "  order_time TIMESTAMP(3)," +
            "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'orders'," +
            "  'properties.bootstrap.servers' = 'localhost:9092'," +
            "  'properties.group.id' = 'flink-orders'," +
            "  'scan.startup.mode' = 'earliest-offset'," +
            "  'format' = 'json'" +
            ")"
        );

        // 创建 JDBC 表
        tableEnv.executeSql(
            "CREATE TABLE customers (" +
            "  customer_id STRING," +
            "  customer_name STRING," +
            "  email STRING," +
            "  PRIMARY KEY (customer_id) NOT ENFORCED" +
            ") WITH (" +
            "  'connector' = 'jdbc'," +
            "  'url' = 'jdbc:mysql://localhost:3306/db'," +
            "  'table-name' = 'customers'," +
            "  'username' = 'root'," +
            "  'password' = 'password'" +
            ")"
        );

        // 创建文件系统表
        tableEnv.executeSql(
            "CREATE TABLE log_files (" +
            "  log_time TIMESTAMP(3)," +
            "  level STRING," +
            "  message STRING," +
            "  row_time AS CAST(log_time AS TIMESTAMP(3))," +
            "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'filesystem'," +
            "  'path' = 'hdfs://namenode:8020/logs'," +
            "  'format' = 'json'" +
            ")"
        );
    }

    /**
     * 2. TableDescriptor 创建表
     */
    public static void createTableWithDescriptor(StreamTableEnvironment tableEnv) {
        // Kafka 表
        tableEnv.connect(
            new Kafka()
                .version("universal")
                .topic("orders")
                .property("bootstrap.servers", "localhost:9092")
                .property("group.id", "flink-orders")
                .startFromEarliest()
        )
        .withFormat(
            new Json()
                .failOnMissingField(false)
                .schema(
                    Schema.newBuilder()
                        .column("order_id", "BIGINT")
                        .column("customer_id", "STRING")
                        .column("amount", "DECIMAL(10, 2)")
                        .build()
                )
        )
        .withSchema(
            Schema.newBuilder()
                .column("order_id", "BIGINT")
                .column("customer_id", "STRING")
                .column("amount", "DECIMAL(10, 2)")
                .build()
        )
        .inAppendMode()
        .createTemporaryTable("orders2");

        // JDBC 表
        tableEnv.connect(
            new Jdbc()
                .driver("com.mysql.cj.jdbc.Driver")
                .dbURL("jdbc:mysql://localhost:3306/db")
                .tableName("customers")
                .username("root")
                .password("password")
        )
        .withFormat(new Json())
        .withSchema(
            Schema.newBuilder()
                .column("customer_id", "STRING")
                .column("customer_name", "STRING")
                .primaryKey("customer_id")
                .build()
        )
        .createTemporaryTable("customers2");
    }

    /**
     * 3. DataStream 转换为 Table
     */
    public static Table fromDataStream(StreamTableEnvironment tableEnv) {
        // 从 DataStream 创建
        DataStream<Order> orderStream = env.fromCollection(getOrders());

        // 方式1: 自动推断
        Table orders = tableEnv.fromDataStream(orderStream);

        // 方式2: 指定字段
        Table ordersWithFields = tableEnv
            .fromDataStream(
                orderStream,
                $("id").rowtime(),
                $("customer"),
                $("amount"),
                $("ts").proctime()
            );

        // 方式3: 注册为视图
        tableEnv.createTemporaryView("orders_stream", orderStream);
        Table ordersView = tableEnv.from("orders_stream");

        return orders;
    }

    /**
     * 4. Table 转换为 DataStream
     */
    public static void toDataStream(StreamTableEnvironment tableEnv) {
        Table orders = tableEnv.sqlQuery("SELECT * FROM orders");

        // 方式1: 追加模式 (AppendStream)
        DataStream<Row> appendStream =
            tableEnv.toAppendStream(orders, Row.class);

        // 方式2: 撤回模式 (RetractStream)
        DataStream<Tuple2<Boolean, Row>> retractStream =
            tableEnv.toRetractStream(orders, Row.class);

        // 方式3: 转换为自定义类型
        DataStream<Order> orderStream =
            tableEnv.toDataStream(orders, Order.class);

        // 方式4: 转换为 Changelog Stream
        DataStream<Row> changelogStream =
            tableEnv.toChangelogStream(orders);
    }
}
```

---

## 查询操作

### SELECT 基本查询

```java
/**
 * SQL 查询操作
 */
public class SQLQueries {

    public static void basicQueries(StreamTableEnvironment tableEnv) {
        // 1. 基本查询
        tableEnv.executeSql(
            "SELECT order_id, customer_id, amount " +
            "FROM orders " +
            "WHERE amount > 100"
        );

        // 2. 聚合查询
        tableEnv.executeSql(
            "SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS order_count " +
            "FROM orders " +
            "GROUP BY customer_id"
        );

        // 3. DISTINCT 查询
        tableEnv.executeSql(
            "SELECT DISTINCT customer_id " +
            "FROM orders " +
            "WHERE amount > 100"
        );

        // 4. LIMIT 查询
        tableEnv.executeSql(
            "SELECT * FROM orders LIMIT 10"
        );

        // 5. ORDER BY + LIMIT
        tableEnv.executeSql(
            "SELECT * FROM orders " +
            "ORDER BY order_time DESC " +
            "LIMIT 100"
        );
    }

    /**
     * 聚合函数
     */
    public static void aggregateFunctions(StreamTableEnvironment tableEnv) {
        // COUNT
        tableEnv.executeSql("SELECT COUNT(*) FROM orders");
        tableEnv.executeSql("SELECT COUNT(DISTINCT customer_id) FROM orders");

        // SUM/AVG/MAX/MIN
        tableEnv.executeSql(
            "SELECT customer_id, " +
            "  SUM(amount) AS total, " +
            "  AVG(amount) AS avg_amount, " +
            "  MAX(amount) AS max_amount, " +
            "  MIN(amount) AS min_amount " +
            "FROM orders GROUP BY customer_id"
        );

        // 方差/标准差
        tableEnv.executeSql(
            "SELECT VAR_POP(amount), VAR_SAMP(amount), STDDEV_POP(amount) " +
            "FROM orders"
        );

        // 集合函数
        tableEnv.executeSql(
            "SELECT COLLECT(customer_id) FROM orders GROUP BY customer_id"
        );
    }

    /**
     * 字符串函数
     */
    public static void stringFunctions(StreamTableEnvironment tableEnv) {
        // CONCAT / CONCAT_WS
        tableEnv.executeSql(
            "SELECT CONCAT(first_name, ' ', last_name) AS full_name " +
            "FROM customers"
        );

        tableEnv.executeSql(
            "SELECT CONCAT_WS(', ', city, state, country) AS location " +
            "FROM customers"
        );

        // SUBSTRING
        tableEnv.executeSql(
            "SELECT SUBSTRING(email, 1, INSTR(email, '@') - 1) AS username " +
            "FROM customers"
        );

        // LIKE / SIMILAR
        tableEnv.executeSql(
            "SELECT * FROM customers " +
            "WHERE email LIKE '%@gmail.com'"
        );

        // 正则匹配
        tableEnv.executeSql(
            "SELECT * FROM customers " +
            "WHERE email SIMILAR TO '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}'"
        );

        // 大小写转换
        tableEnv.executeSql(
            "SELECT UPPER(name), LOWER(email) FROM customers"
        );

        // TRIM / LTRIM / RTRIM
        tableEnv.executeSql(
            "SELECT TRIM(BOTH ' ' FROM message) FROM logs"
        );
    }

    /**
     * 时间函数
     */
    public static void timeFunctions(StreamTableEnvironment tableEnv) {
        // CURRENT_TIMESTAMP / CURRENT_DATE
        tableEnv.executeSql(
            "SELECT CURRENT_TIMESTAMP, CURRENT_DATE FROM orders"
        );

        // 时间提取
        tableEnv.executeSql(
            "SELECT " +
            "  YEAR(order_time) AS year, " +
            "  MONTH(order_time) AS month, " +
            "  DAY(order_time) AS day, " +
            "  HOUR(order_time) AS hour, " +
            "  MINUTE(order_time) AS minute, " +
            "  SECOND(order_time) AS second " +
            "FROM orders"
        );

        // 时间加减
        tableEnv.executeSql(
            "SELECT " +
            "  order_time + INTERVAL '1' DAY AS next_day, " +
            "  order_time - INTERVAL '1' HOUR AS previous_hour " +
            "FROM orders"
        );

        // 时间戳运算
        tableEnv.executeSql(
            "SELECT " +
            "  TIMESTAMP_DIFF(SECOND, order_time, NOW()) AS elapsed_seconds " +
            "FROM orders"
        );

        // 时区转换
        tableEnv.executeSql(
            "SELECT " +
            "  order_time AT TIME ZONE 'UTC' AS utc_time, " +
            "  order_time AT TIME ZONE 'Asia/Shanghai' AS china_time " +
            "FROM orders"
        );
    }

    /**
     * 条件函数
     */
    public static void conditionalFunctions(StreamTableEnvironment tableEnv) {
        // CASE WHEN
        tableEnv.executeSql(
            "SELECT " +
            "  order_id, " +
            "  amount, " +
            "  CASE " +
            "    WHEN amount > 1000 THEN 'VIP' " +
            "    WHEN amount > 500 THEN 'Premium' " +
            "    ELSE 'Regular' " +
            "  END AS customer_tier " +
            "FROM orders"
        );

        // NULL 相关
        tableEnv.executeSql(
            "SELECT COALESCE(email, 'unknown') FROM customers"
        );

        tableEnv.executeSql(
            "SELECT NULLIF(amount, 0) FROM orders"
        );

        // IF 函数
        tableEnv.executeSql(
            "SELECT IF(amount > 1000, 'high', 'low') AS amount_level " +
            "FROM orders"
        );
    }
}
```

---

## 窗口聚合

### 窗口类型

```java
/**
 * 窗口聚合查询
 */
public class WindowAggregation {

    public static void tumblingWindow(StreamTableEnvironment tableEnv) {
        // 1. 滚动窗口 (Tumbling Window)
        tableEnv.executeSql(
            "SELECT " +
            "  customer_id, " +
            "  TUMBLE_START(order_time, INTERVAL '1' HOUR) AS window_start, " +
            "  TUMBLE_END(order_time, INTERVAL '1' HOUR) AS window_end, " +
            "  SUM(amount) AS total_amount, " +
            "  COUNT(*) AS order_count " +
            "FROM orders " +
            "GROUP BY " +
            "  customer_id, " +
            "  TUMBLE(order_time, INTERVAL '1' HOUR)"
        );

        // 使用窗口 TVF (Table-valued Function) - Flink 1.13+
        tableEnv.executeSql(
            "SELECT " +
            "  customer_id, " +
            "  window_start, " +
            "  window_end, " +
            "  SUM(amount) AS total_amount " +
            "FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' HOUR)) " +
            "GROUP BY customer_id, window_start, window_end"
        );
    }

    public static void slidingWindow(StreamTableEnvironment tableEnv) {
        // 2. 滑动窗口 (Sliding Window)
        tableEnv.executeSql(
            "SELECT " +
            "  customer_id, " +
            "  HOP_START(order_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) AS window_start, " +
            "  HOP_END(order_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) AS window_end, " +
            "  SUM(amount) AS total_amount " +
            "FROM orders " +
            "GROUP BY " +
            "  customer_id, " +
            "  HOP(order_time, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)"
        );
    }

    public static void sessionWindow(StreamTableEnvironment tableEnv) {
        // 3. 会话窗口 (Session Window)
        tableEnv.executeSql(
            "SELECT " +
            "  customer_id, " +
            "  SESSION_START(order_time, INTERVAL '30' MINUTE) AS window_start, " +
            "  SESSION_END(order_time, INTERVAL '30' MINUTE) AS window_end, " +
            "  SUM(amount) AS total_amount " +
            "FROM orders " +
            "GROUP BY " +
            "  customer_id, " +
            "  SESSION(order_time, INTERVAL '30' MINUTE)"
        );
    }

    public static void cumulativeWindow(StreamTableEnvironment tableEnv) {
        // 4. 累计窗口 (Cumulative Window) - Flink 1.14+
        tableEnv.executeSql(
            "SELECT " +
            "  customer_id, " +
            "  window_start, " +
            "  window_end, " +
            "  SUM(amount) AS total_amount " +
            "FROM TABLE(CUMULATE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' HOUR, INTERVAL '24' HOUR)) " +
            "GROUP BY customer_id, window_start, window_end"
        );
    }
}
```

### 窗口聚合函数

```java
/**
 * 窗口聚合相关函数
 */
public class WindowAggregateFunctions {

    public static void windowFunctions(StreamTableEnvironment tableEnv) {
        // 窗口开始/结束时间
        tableEnv.executeSql(
            "SELECT " +
            "  window_start, " +
            "  window_end, " +
            "  window_time AS event_time " +
            "FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' HOUR))"
        );

        // 窗口行数
        tableEnv.executeSql(
            "SELECT " +
            "  window_start, " +
            "  COUNT(*) AS row_count " +
            "FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' HOUR)) " +
            "GROUP BY window_start"
        );
    }

    public static void deduplication(StreamTableEnvironment tableEnv) {
        // 去重 (使用窗口)
        tableEnv.executeSql(
            "SELECT order_id, customer_id, amount " +
            "FROM (" +
            "  SELECT *, " +
            "    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_time DESC) AS rn " +
            "  FROM orders" +
            ") WHERE rn = 1"
        );

        // 无限状态去重 (使用 Deduplicate 策略)
        tableEnv.executeSql(
            "SELECT * FROM orders " +
            "ORDER BY order_time " +
            "PROCTIME() AS proc_time " +
            "DEDUPLICATE KEEP LAST ROW"
        );
    }

    public static void topN(StreamTableEnvironment tableEnv) {
        // Top N 查询
        tableEnv.executeSql(
            "SELECT * FROM (" +
            "  SELECT *, " +
            "    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS rn " +
            "  FROM orders" +
            ") WHERE rn <= 5"
        );

        // 窗口 Top N
        tableEnv.executeSql(
            "SELECT * FROM (" +
            "  SELECT *, " +
            "    ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY amount DESC) AS rn " +
            "  FROM (" +
            "    SELECT " +
            "      window_start, " +
            "      order_id, " +
            "      amount " +
            "    FROM TABLE(TUMBLE(TABLE orders, DESCRIPTOR(order_time), INTERVAL '1' HOUR))" +
            "  )" +
            ") WHERE rn <= 10"
        );
    }
}
```

---

## 连接操作

### 表连接

```java
/**
 * 表连接操作
 */
public class TableJoins {

    public static void regularJoin(StreamTableEnvironment tableEnv) {
        // 1. 等值连接 (Regular Join)
        // 支持 INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN
        tableEnv.executeSql(
            "SELECT o.order_id, o.amount, c.customer_name " +
            "FROM orders o " +
            "JOIN customers c ON o.customer_id = c.customer_id"
        );

        // 多表连接
        tableEnv.executeSql(
            "SELECT o.order_id, p.product_name, s.shipment_date " +
            "FROM orders o " +
            "JOIN products p ON o.product_id = p.product_id " +
            "JOIN shipments s ON o.order_id = s.order_id"
        );

        // LEFT JOIN
        tableEnv.executeSql(
            "SELECT o.order_id, o.amount, s.shipment_id " +
            "FROM orders o " +
            "LEFT JOIN shipments s ON o.order_id = s.order_id"
        );
    }

    public static void intervalJoin(StreamTableEnvironment tableEnv) {
        // 2. 区间连接 (Interval Join)
        // 基于时间范围的连接
        tableEnv.executeSql(
            "SELECT o.order_id, p.payment_id " +
            "FROM orders o, payments p " +
            "WHERE o.order_id = p.order_id " +
            "  AND p.payment_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR"
        );

        // 使用窗口 TVF 的区间连接
        tableEnv.executeSql(
            "SELECT * FROM (" +
            "  SELECT " +
            "    o.order_id, " +
            "    o.order_time, " +
            "    p.payment_id, " +
            "    p.payment_time " +
            "  FROM orders o " +
            "  JOIN payments p ON o.order_id = p.order_id " +
            "  WHERE p.payment_time BETWEEN o.order_time AND o.order_time + INTERVAL '1' HOUR" +
            ")"
        );
    }

    public static void temporalJoin(StreamTableEnvironment tableEnv) {
        // 3. 时间连接 (Temporal Join)
        // 连接维度表 (带有版本号的表)
        tableEnv.executeSql(
            "SELECT o.order_id, o.amount, r.rate " +
            "FROM orders o " +
            "JOIN rates FOR SYSTEM_TIME AS OF o.order_time AS r " +
            "ON o.currency = r.currency"
        );

        // LOOKUP JOIN (查找连接)
        tableEnv.executeSql(
            "SELECT " +
            "  o.order_id, " +
            "  o.amount, " +
            "  c.customer_name " +
            "FROM orders o " +
            "JOIN customers FOR SYSTEM_TIME AS OF o.proc_time AS c " +
            "ON o.customer_id = c.customer_id"
        );
    }

    public static void arrayJoin(StreamTableEnvironment tableEnv) {
        // 4. 数组连接 (Array Join)
        tableEnv.executeSql(
            "SELECT * FROM orders, UNNEST(tags) AS tag"
        );

        // 生成系列
        tableEnv.executeSql(
            "SELECT i " +
            "FROM UNNEST(SEQUENCE(1, 100)) AS i"
        );
    }

    public static void setOperations(StreamTableEnvironment tableEnv) {
        // 5. 集合操作
        tableEnv.executeSql(
            "SELECT order_id FROM orders_2024 " +
            "UNION ALL " +
            "SELECT order_id FROM orders_2025"
        );

        tableEnv.executeSql(
            "SELECT customer_id FROM orders_2024 " +
            "UNION " +
            "SELECT customer_id FROM orders_2025"
        );

        tableEnv.executeSql(
            "SELECT customer_id FROM orders_2024 " +
            "INTERSECT ALL " +
            "SELECT customer_id FROM orders_2025"
        );

        tableEnv.executeSql(
            "SELECT customer_id FROM orders_2024 " +
            "EXCEPT ALL " +
            "SELECT customer_id FROM cancelled_orders"
        );
    }
}
```

---

## DML 操作

### INSERT

```java
/**
 * DML 操作
 */
public class DMLOperations {

    public static void insertInto(StreamTableEnvironment tableEnv) {
        // 1. INSERT INTO 表
        tableEnv.executeSql(
            "INSERT INTO orders_enriched " +
            "SELECT o.order_id, o.amount, c.customer_name " +
            "FROM orders o " +
            "JOIN customers c ON o.customer_id = c.customer_id"
        );

        // 2. 多表输出
        // 输出到多个 Sink
        tableEnv.executeSql(
            "INSERT INTO high_value_orders " +
            "SELECT * FROM orders WHERE amount > 1000"
        );

        tableEnv.executeSql(
            "INSERT INTO vip_customers " +
            "SELECT customer_id, SUM(amount) AS total " +
            "FROM orders " +
            "GROUP BY customer_id " +
            "HAVING SUM(amount) > 10000"
        );

        // 3. 动态表插入
        // 使用 Table API
        tableEnv.executeSql(
            "INSERT INTO sink_table " +
            "SELECT * FROM source_table"
        ).await();
    }

    public static void update(StreamTableEnvironment tableEnv) {
        // UPDATE (需要主键)
        tableEnv.executeSql(
            "UPDATE orders " +
            "SET status = 'completed' " +
            "WHERE order_id = 12345"
        );

        // DELETE (需要主键)
        tableEnv.executeSql(
            "DELETE FROM orders " +
            "WHERE status = 'cancelled' " +
            "  AND order_time < TIMESTAMP '2024-01-01 00:00:00'"
        );
    }
}
```

---

## 类型系统

### Flink SQL 类型

```java
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

/**
 * Flink SQL 数据类型
 */
public class DataTypesExample {

    /**
     * 常用数据类型
     */
    public static void commonTypes() {
        // 字符串类型
        DataTypes.STRING()      // 可变长度字符串
        DataTypes.VARCHAR(100)  // 定长字符串
        DataTypes.CHAR(10)      // 定长字符
        DataTypes.TEXT()        // 大文本

        // 数值类型
        DataTypes.TINYINT()     // 1字节整数
        DataTypes.SMALLINT()    // 2字节整数
        DataTypes.INT()         // 4字节整数
        DataTypes.BIGINT()      // 8字节整数
        DataTypes.FLOAT()       // 单精度浮点
        DataTypes.DOUBLE()      // 双精度浮点
        DataTypes.DECIMAL(10, 2) // 精确数值

        // 时间类型
        DataTypes.DATE()        // 日期
        DataTypes.TIME()        // 时间
        DataTypes.TIMESTAMP(3)  // 时间戳 (毫秒)
        DataTypes.TIMESTAMP_LTZ(3) // 带时区时间戳
        DataTypes.INTERVAL(DataTypes.MONTH())  // 月间隔
        DataTypes.INTERVAL(DataTypes.SECOND()) // 秒间隔

        // 布尔类型
        DataTypes.BOOLEAN()

        // 二进制类型
        DataTypes.BINARY(100)   // 二进制
        DataTypes.VARBINARY(1000) // 可变二进制
        DataTypes.BYTES()       // 字节数组

        // 复杂类型
        DataTypes.ARRAY(DataTypes.STRING())  // 数组
        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()) // 映射
        DataTypes.ROW(
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("age", DataTypes.INT())
        ) // 行类型
    }

    /**
     * Schema 定义
     */
    public static void schemaDefinition() {
        // 方式1: Builder 模式
        Schema schema = Schema.newBuilder()
            .column("order_id", DataTypes.BIGINT().notNull())
            .column("customer_id", DataTypes.STRING())
            .column("amount", DataTypes.DECIMAL(10, 2))
            .column("order_time", DataTypes.TIMESTAMP(3))
            .column("proc_time", DataTypes.PROCTIME())
            .column("row_time", DataTypes.ROWTIME())
            .primaryKey("order_id")
            .build();

        // 方式2: DDL 字符串
        String ddl = "CREATE TABLE orders (" +
            "  order_id BIGINT NOT NULL," +
            "  customer_id STRING," +
            "  amount DECIMAL(10, 2)," +
            "  order_time TIMESTAMP(3)," +
            "  proc_time AS PROCTIME()," +
            "  row_time AS CAST(order_time AS TIMESTAMP(3))," +
            "  PRIMARY KEY (order_id) NOT ENFORCED" +
            ") WITH (...)";
    }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Flink 架构详解](01-architecture.md) | Flink 核心架构和概念 |
| [Flink DataStream API](02-datastream.md) | DataStream API 详解 |
| [Flink 状态管理](04-state-checkpoint.md) | 状态管理和 Checkpoint |
| [Flink CEP](05-cep.md) | 复杂事件处理 |
| [Flink 运维指南](06-operations.md) | 集群部署和运维 |
